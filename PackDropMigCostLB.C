/***
  * Author: Vinicius Freitas
  * contact: vinicius.mct.freitas@gmail.com OR vinicius.mctf@grad.ufsc.br
  * Produced @ ECL - UFSC
  * Newly developed strategy based on Harshita Menon's implementation of GrapevineLB  
  */

#include "PackDropMigCostLB.h"
#include "OrderedElement.h"
#include "elements.h"
#include <stdlib.h>
#include <cstring>
#include <algorithm>

#ifndef PD_LB_MIG_COST
#define PD_LB_PD_LB_MIG_COST 1.3
#endif
  
CreateLBFunc_Def(PackDropMigCostLB, "The distributed pack-based load balancer accounting for migration costs");

PackDropMigCostLB::PackDropMigCostLB(CkMigrateMessage *m) : CBase_PackDropMigCostLB(m) {
}

PackDropMigCostLB::PackDropMigCostLB(const CkLBOptions &opt) : CBase_PackDropMigCostLB(opt) {
  lbname = "PackDropMigCostLB";
  if (CkMyPe() == 0)
    CkPrintf("[%d] PackDropMigCostLB created\n",CkMyPe());
  InitLB(opt);
}

void PackDropMigCostLB::InitLB(const CkLBOptions &opt) {
  thisProxy = CProxy_PackDropMigCostLB(thisgroup);
}

void PackDropMigCostLB::Strategy(const DistBaseLB::LDStats* const stats) {
    if (CkMyPe() == 0) {
        CkPrintf("In PackDrop Strategy\n");
    }

    lb_started = false;
    my_stats = stats;
    threshold = 0.05;
    lb_end = false;
    tries = 0;
    info_send_count = 0;

    packs.clear();
    pe_no.clear();
    loads.clear();
    receivers.clear();
    pack_count = 0;
    total_migrates = 0;
    acks_needed = 0;
    
    kMaxGossipMsgCount = 2 * CmiLog2(CkNumPes());
    kPartialInfoCount = -1;
    
    local_tasks = std::priority_queue<Element, std::deque<Element>>();
    local_tasks.clear();
    srand((unsigned)CmiWallTimer()*CkMyPe()/CkNumPes());
    
    my_load = 0;
    for (int i = 0; i < my_stats->n_objs; ++i) {
        if (my_stats->objData[i].migratable) {
            local_tasks.emplace(i, my_stats->objData[i].wallTime);
            my_load += my_stats->objData[i].wallTime;
        }
    }
    CkCallback cb(CkReductionTarget(PackDropMigCostLB, Load_Setup), thisProxy);
    contribute(sizeof(double), &my_load, CkReduction::sum_double, cb);
}

void PackDropMigCostLB::Load_Setup(double total_load) {
    avg_load = total_load/CkNumPes();
    int chares = my_stats->n_objs;

    CkCallback cb(CkReductionTarget(PackDropMigCostLB, Chare_Setup),thisProxy);
    contribute(sizeof(int), &chares, CkReduction::sum_int, cb);
}


void PackDropMigCostLB::Chare_Setup(int count) {
    chare_count = count;
    double avg_task_size = (CkNumPes()*avg_load)/chare_count;
    pack_load = avg_task_size*(2 - CkNumPes()/chare_count);
    
    double ceil = avg_load*(1+threshold);
    double pack_floor = pack_load*(1-threshold);
    double pack_load_now = 0;
    if (my_load > ceil) {
        int pack_id = 0;
        packs[pack_id] = std::vector<int>();
        while (my_load > ceil) {
            Element t = local_tasks.top();
            packs[pack_id].push_back(t.id);
            pack_load_now += t.load;
            my_load -= t.load;
            local_tasks.pop();
            if (pack_load_now > pack_floor) {
                pack_id++;
                packs[pack_id] = std::vector<int>();
            }
        }
        pack_count = packs.size();
    } else {
        double r_loads[1];
        int r_pe_no[1];
        r_loads[0] = my_load;
        r_pe_no[0] = CkMyPe();
        req_hop = 0;
        GossipLoadInfo(req_hop, CkMyPe(), 1, r_pe_no, r_loads);
    }
    if (CkMyPe() == 0) {
        CkCallback cb(CkIndex_PackDropMigCostLB::First_Barrier(), thisProxy);
        CkStartQD(cb);
    }
}

void PackDropMigCostLB::First_Barrier() {
    LoadBalance();
}

void PackDropMigCostLB::LoadBalance() {
    lb_started = true;
    if (packs.size() == 0) {
        msg = new(total_migrates,CkNumPes(),CkNumPes(),0) LBMigrateMsg;
        msg->n_moves = total_migrates;
        contribute(CkCallback(CkReductionTarget(PackDropMigCostLB, Final_Barrier), thisProxy));
        return;
    }
    CalculateReceivers();
    PackSend();
}

void PackDropMigCostLB::CalculateReceivers() {
    double pack_ceil = pack_load*(1+threshold);
    double ceil = avg_load*(1+threshold);
    for (size_t i = 0; i < pe_no.size(); ++i) {
        if (loads[i] + pack_ceil < ceil) {
            receivers.push_back(pe_no[i]);
        }
    }
}

int PackDropMigCostLB::FindReceiver() {
    int rec;
    if (receivers.size() < CkNumPes()/4) {
        rec = rand()%CkNumPes();
        while (rec == CkMyPe()) {
            rec = rand()%CkNumPes();
        }
    } else {
        rec = receivers[rand()%receivers.size()];
        while (rec == CkMyPe()) {
            rec = receivers[rand()%receivers.size()];
        } 
    }
    return rec;
}

void PackDropMigCostLB::PackSend(int pack_id, int one_time) {
    tries++;
    if (tries >= 4) {
        if (_lb_args.debug()) CkPrintf("[%d] No receivers found\n", CkMyPe());
        EndStep();
        return;
    }
    int idp = pack_id;
    while (idp < packs.size()) {
        if (packs[idp].empty()) {
            ++idp;
            continue;
        }
        int rand_rec = FindReceiver();
        
        acks_needed++;
        thisProxy[rand_rec].PackAck(idp, CkMyPe(), packs[idp].size(), false);
        
        if (one_time) {
            break;
        }
        ++idp;
    }
}

/*
 * This is the main method modified in this version.
 * Migration costs are applyed by multiplying the number of tasks coming in a pack.
 * The more packs a node receive, more it is comunicating with outside entities.
 * This will make the tasks heavier in the long term.
 * The weight of PD_LB_MIG_COST must be evaluated from application to application.
 */
void PackDropMigCostLB::PackAck(int id, int from, int psize, bool force) {
    bool ack = ((my_load + pack_load < avg_load*(1+threshold)) || force);
    if (ack) {
        migrates_expected+=psize;
        my_load += pack_load*(psize*PD_LB_MIG_COST); 
    }
    thisProxy[from].RecvAck(id, CkMyPe(), ack);
}

void PackDropMigCostLB::RecvAck(int id, int to, bool success) {
    if (success) {
        const std::vector<int> this_pack = packs.at(id);
        for (size_t i = 0; i < this_pack.size(); ++i) {
            int task = this_pack.at(i);
            MigrateInfo* inf = new MigrateInfo();
            inf->obj = my_stats->objData[task].handle;
            inf->from_pe = CkMyPe();
            inf->to_pe = to;
            migrateInfo.push_back(inf);
        }
        packs[id] = std::vector<int>();
        total_migrates++;
        acks_needed--;
        pack_count--;
        if (acks_needed == 0) {
            msg = new(total_migrates, CkNumPes(), CkNumPes(), 0) LBMigrateMsg;
            msg->n_moves = total_migrates;
            for (size_t i = 0; i < total_migrates; ++i) {
                MigrateInfo* inf = (MigrateInfo*) migrateInfo[i];
                msg->moves[i] = *inf;
                delete inf;
            }
            migrateInfo.clear();
            lb_end = true;
            contribute(CkCallback(CkReductionTarget(PackDropMigCostLB, Final_Barrier), thisProxy));
        }
    } else {
        acks_needed--;
        if (tries >= 2) {
            ForcedPackSend(id, true);
        } else {
            ForcedPackSend(id, false);
        }
    }
}

void PackDropMigCostLB::ForcedPackSend(int id, bool force) {
    int rand_rec = FindReceiver();
    tries++;
    acks_needed++;
    thisProxy[rand_rec].PackAck(id, CkMyPe(), packs.at(id).size(), force);
}

void PackDropMigCostLB::EndStep() {
    if (total_migrates < pack_count && tries < 8) {
        CkPrintf("[%d] Gotta migrate more: %d\n", CkMyPe(), tries);
        PackSend();
    } else {
        msg = new(total_migrates, CkNumPes(), CkNumPes(), 0) LBMigrateMsg;
        msg->n_moves = total_migrates;
        for (size_t i = 0; i < total_migrates; ++i) {
            MigrateInfo* inf = (MigrateInfo*) migrateInfo[i];
            msg->moves[i] = *inf;
            delete inf;
        }
        migrateInfo.clear();
        lb_end = true;
        contribute(CkCallback(CkReductionTarget(PackDropMigCostLB, Final_Barrier), thisProxy));
    }
}

void PackDropMigCostLB::Final_Barrier() {
    ProcessMigrationDecision(msg);
}

// TODO
void PackDropMigCostLB::ShowMigrationDetails() {
  if (total_migrates > 0)
    CkPrintf("[%d] migrating %d elements\n", CkMyPe(), total_migrates);
  if (migrates_expected > 0) 
    CkPrintf("[%d] receiving %d elements\n", CkMyPe(), migrates_expected);
  
  CkCallback cb (CkReductionTarget(PackDropMigCostLB, DetailsRedux), thisProxy);
  contribute(sizeof(int), &total_migrates, CkReduction::sum_int, cb);
}

void PackDropMigCostLB::DetailsRedux(int migs) {
  if (CkMyPe() <= 0) CkPrintf("[%d] Total number of migrations is %d\n", CkMyPe(), migs);
}

/*
* Gossip load information between peers. Receive the gossip message.
*/
void PackDropMigCostLB::GossipLoadInfo(int req_h, int from_pe, int n,
    int remote_pe_no[], double remote_loads[]) {
  std::vector<int> p_no;
  std::vector<double> l;

  int i = 0;
  int j = 0;
  int m = pe_no.size();

  while (i < m && j < n) {
    if (pe_no[i] < remote_pe_no[j]) {
      p_no.push_back(pe_no[i]);
      l.push_back(loads[i]);
      i++;
    } else {
      p_no.push_back(remote_pe_no[j]);
      l.push_back(remote_loads[j]);
      if (pe_no[i] == remote_pe_no[j]) {
        i++;
      }
      j++;
    }
  }

  if (i == m && j != n) {
    while (j < n) {
      p_no.push_back(remote_pe_no[j]);
      l.push_back(remote_loads[j]);
      j++;
    }
  } else if (j == n && i != m) {
    while (i < m) {
      p_no.push_back(pe_no[i]);
      l.push_back(loads[i]);
      i++;
    }
  }

  pe_no.swap(p_no);
  loads.swap(l);
  req_hop = req_h + 1;

  SendLoadInfo();
}

/*
* Construct the gossip message and send to peers
*/
void PackDropMigCostLB::SendLoadInfo() {
  if (gossip_msg_count > kMaxGossipMsgCount) {
    return;
  }

  int rand_nbor1;
  int rand_nbor2 = -1;
  do {
    rand_nbor1 = rand() % CkNumPes();
  } while (rand_nbor1 == CkMyPe());
  
  do {
    rand_nbor2 = rand() % CkNumPes();
  } while ((rand_nbor2 == CkMyPe()) || (rand_nbor2 == rand_nbor1));

  
  int info_count = (kPartialInfoCount >= 0) ? kPartialInfoCount : pe_no.size();
  int* p = new int[info_count];
  double* l = new double[info_count];
  for (int i = 0; i < info_count; i++) {
    p[i] = pe_no[i];
    l[i] = loads[i];
  }

  thisProxy[rand_nbor1].GossipLoadInfo(req_hop, CkMyPe(), info_count, p, l);
  thisProxy[rand_nbor2].GossipLoadInfo(req_hop, CkMyPe(), info_count, p, l);

  gossip_msg_count++;

  delete[] p;
  delete[] l;
}

#include "PackDropMigCostLB.def.h"
