/***
  * Author: Vinicius Freitas
  * contact: vinicius.mct.freitas@gmail.com OR vinicius.mctf@grad.ufsc.br
  * Produced @ ECL - UFSC
  * Newly developed strategy based on Harshita Menon's implementation of GrapevineLB
  */

#include "InformedPackDropLB.h"
#include "OrderedElement.h"
#include "elements.h"
#include <stdlib.h>
#include <cstring>
#include <algorithm>

#ifndef PD_LB_MIG_COST
#define PD_LB_MIG_COST 8.0
#endif

CreateLBFunc_Def(InformedPackDropLB, "The distributed pack-based load balancer accounting for migration costs");

InformedPackDropLB::InformedPackDropLB(CkMigrateMessage *m) : CBase_InformedPackDropLB(m) {
}

InformedPackDropLB::InformedPackDropLB(const CkLBOptions &opt) : CBase_InformedPackDropLB(opt) {
  lbname = "InformedPackDropLB";
  if (CkMyPe() == 0)
    CkPrintf("[%d] InformedPackDropLB created\n",CkMyPe());
  InitLB(opt);
}

void InformedPackDropLB::InitLB(const CkLBOptions &opt) {
  thisProxy = CProxy_InformedPackDropLB(thisgroup);
}

void InformedPackDropLB::Strategy(const DistBaseLB::LDStats* const stats) {
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
    CkCallback cb(CkReductionTarget(InformedPackDropLB, Load_Setup), thisProxy);
    contribute(sizeof(double), &my_load, CkReduction::sum_double, cb);
}

void InformedPackDropLB::Load_Setup(double total_load) {
    avg_load = total_load/CkNumPes();
    int chares = my_stats->n_objs;

    CkCallback cb(CkReductionTarget(InformedPackDropLB, Chare_Setup),thisProxy);
    contribute(sizeof(int), &chares, CkReduction::sum_int, cb);
}


void InformedPackDropLB::Chare_Setup(int count) {
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
        CkCallback cb(CkIndex_InformedPackDropLB::First_Barrier(), thisProxy);
        CkStartQD(cb);
    }
}

void InformedPackDropLB::First_Barrier() {
    LoadBalance();
}

void InformedPackDropLB::LoadBalance() {
    lb_started = true;
    if (packs.size() == 0) {
        msg = new(total_migrates,CkNumPes(),CkNumPes(),0) LBMigrateMsg;
        msg->n_moves = total_migrates;
        contribute(CkCallback(CkReductionTarget(InformedPackDropLB, Final_Barrier), thisProxy));
        return;
    }
    CalculateReceivers();
    PackSend();
}

void InformedPackDropLB::CalculateReceivers() {
    double pack_ceil = pack_load*(1+threshold);
    double ceil = avg_load*(1+threshold);
    for (size_t i = 0; i < pe_no.size(); ++i) {
        if (loads[i] + pack_ceil < ceil) {
            receivers.push_back(pe_no[i]);
        }
    }

    // The min loaded PEs have probabilities inversely proportional to their load.
    double cumulative = 0.0;
    int underloaded_pe_count = receivers.size();
    distribution.clear();
    distribution.reserve(underloaded_pe_count);
    for (int i = 0; i < underloaded_pe_count; i++) {
      cumulative += (thr_avg - loads[i])/thr_avg;
      distribution.push_back(cumulative);
    }

    for (int i = 0; i < underloaded_pe_count; i++) {
      distribution[i] = distribution[i]/cumulative;
    }
}

int InformedPackDropLB::FindReceiver() {
  double no = (double) rand()/(double) RAND_MAX;
  for (int i = 0; i < distribution.size(); i++) {
    if (distribution[i] >= no) {
      return i;
    }
  }
  return -1;
}

void InformedPackDropLB::PackSend(int pack_id, int one_time) {
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

double InformedPackDropLB::RecalculateLoad(int n_tasks) {
  double new_load = pack_load;
  if (n_tasks > 1) {
    new_load += new_load*(n_tasks*PD_LB_MIG_COST)/64.0;
  }
  return new_load;
}

/*
 * This is the main method modified in this version.
 * Migration costs are applyed by multiplying the number of tasks coming in a pack.
 * The more packs a node receive, more it is comunicating with outside entities.
 * This will make the tasks heavier in the long term.
 * The weight of PD_LB_MIG_COST must be evaluated from application to application.
 */
void InformedPackDropLB::PackAck(int id, int from, int psize, bool force) {
    bool ack = ((my_load + RecalculateLoad(psize) < avg_load*(1+threshold)) || force);
    if (ack) {
        migrates_expected+=psize;
        my_load += RecalculateLoad(psize);
    }
    thisProxy[from].RecvAck(id, CkMyPe(), ack);
}

void InformedPackDropLB::RecvAck(int id, int to, bool success) {
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
            contribute(CkCallback(CkReductionTarget(InformedPackDropLB, Final_Barrier), thisProxy));
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

void InformedPackDropLB::ForcedPackSend(int id, bool force) {
    int rand_rec = FindReceiver();
    tries++;
    acks_needed++;
    thisProxy[rand_rec].PackAck(id, CkMyPe(), packs.at(id).size(), force);
}

void InformedPackDropLB::EndStep() {
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
        contribute(CkCallback(CkReductionTarget(InformedPackDropLB, Final_Barrier), thisProxy));
    }
}

void InformedPackDropLB::Final_Barrier() {
    ProcessMigrationDecision(msg);
}

// TODO
void InformedPackDropLB::ShowMigrationDetails() {
  if (total_migrates > 0)
    CkPrintf("[%d] migrating %d elements\n", CkMyPe(), total_migrates);
  if (migrates_expected > 0)
    CkPrintf("[%d] receiving %d elements\n", CkMyPe(), migrates_expected);

  CkCallback cb (CkReductionTarget(InformedPackDropLB, DetailsRedux), thisProxy);
  contribute(sizeof(int), &total_migrates, CkReduction::sum_int, cb);
}

void InformedPackDropLB::DetailsRedux(int migs) {
  if (CkMyPe() <= 0) CkPrintf("[%d] Total number of migrations is %d\n", CkMyPe(), migs);
}

/*
* Gossip load information between peers. Receive the gossip message.
*/
void InformedPackDropLB::GossipLoadInfo(int req_h, int from_pe, int n,
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
void InformedPackDropLB::SendLoadInfo() {
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

#include "InformedPackDropLB.def.h"
