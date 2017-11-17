/***
  * Author: Vinicius Freitas
  * contact: vinicius.mct.freitas@gmail.com OR vinicius.mctf@grad.ufsc.br
  * Produced @ ECL - UFSC
  * Newly developed strategy based on Harshita Menon's implementation of GrapevineLB  
  */

#include "PackDropLB.h"
#include "OrderedElement.h"
#include "elements.h"
#include <stdlib.h>
#include <cstring>
#include <algorithm>
  
CreateLBFunc_Def(PackDropLB, "The distributed load balancer");

PackDropLB::PackDropLB(CkMigrateMessage *m) : CBase_PackDropLB(m) {
}

PackDropLB::PackDropLB(const CkLBOptions &opt) : CBase_PackDropLB(opt) {
  lbname = "PackDropLB";
  if (CkMyPe() == 0)
    CkPrintf("[%d] PackDropLB created\n",CkMyPe());
  InitLB(opt);
}

void PackDropLB::InitLB(const CkLBOptions &opt) {
  thisProxy = CProxy_PackDropLB(thisgroup);
}

void PackDropLB::Strategy(const DistBaseLB::LDStats* const stats) {
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
    pack_count = 0;
    total_migrates = 0;
    acks_needed = 0;
    kMaxGossipMsgCount = 2 * CmiLog2(CkNumPes());
    kPartialInfoCount = -1;
    receivers.clear();
    local_tasks = std::priority_queue<Element, std::deque<Element>>();
    srand((unsigned)CmiWallTimer()*CkMyPe()/CkNumPes());
    
    //local_tasks.reserve(my_stat->n_objs);
    my_load = 0;
    for (int i = 0; i < my_stats->n_objs; ++i) {
        if (my_stats->objData[i].migratable) {
            local_tasks.emplace(i, my_stats->objData[i].wallTime);
            my_load += my_stats->objData[i].wallTime;
        }
    }
    CkCallback cb(CkReductionTarget(PackDropLB, Load_Setup), thisProxy);
    contribute(sizeof(double), &my_load, CkReduction::sum_double, cb);
}

void PackDropLB::Load_Setup(double total_load) {
    // Calculating the average load of a pe, based on the total system load.
    avg_load = total_load/CkNumPes();
    int chares = my_stats->n_objs;

    CkCallback cb(CkReductionTarget(PackDropLB, Chare_Setup),thisProxy);
    contribute(sizeof(int), &chares, CkReduction::sum_int, cb);
}


void PackDropLB::Chare_Setup(int count) {
    chare_count = count;
    // packs.reserve(count/CkNumPes());
    
    // Calc Pack Size
    double avg_task_size = (CkNumPes()*avg_load)/chare_count;
    pack_load = avg_task_size*(2 - CkNumPes()/chare_count);
    
    // Mount Packs
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
        //PackSend();
    } else {
        // Scatter my pe_id
        double r_loads[1];
        int r_pe_no[1];
        r_loads[0] = my_load;
        r_pe_no[0] = CkMyPe();
        // Initialize the req_hop of the message to 0
        req_hop = 0;
        GossipLoadInfo(req_hop, CkMyPe(), 1, r_pe_no, r_loads);
        //EndStep();
    }
    if (CkMyPe() == 0) {
        //CkPrintf("Starting QD\n");
        CkCallback cb(CkIndex_PackDropLB::First_Barrier(), thisProxy);
        CkStartQD(cb);
    }
}

void PackDropLB::First_Barrier() {
    //if (CkMyPe() == 0) CkPrintf("Done gossiping\n");
    LoadBalance();
}

void PackDropLB::LoadBalance() {
    // End of the comunication stage
    // if (CkMyPe() == 0) CkPrintf("-----------barrier-----------\n");
    lb_started = true;
    if (packs.size() == 0) {
        msg = new(total_migrates,CkNumPes(),CkNumPes(),0) LBMigrateMsg;
        msg->n_moves = total_migrates;
        //CkPrintf("[%d] I have contributed // UNDERLOADED //\n", CkMyPe());
        contribute(CkCallback(CkReductionTarget(PackDropLB, Final_Barrier), thisProxy));
        return;
    }
    CalculateReceivers();
    PackSend();
}

void PackDropLB::CalculateReceivers() {
    double pack_ceil = pack_load*(1+threshold);
    double ceil = avg_load*(1+threshold);
    for (size_t i = 0; i < pe_no.size(); ++i) {
        if (loads[i] + pack_ceil < ceil) {
            receivers.push_back(pe_no[i]);
        }
    }
}

int PackDropLB::FindReceiver() {
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

void PackDropLB::PackSend(int pack_id, int one_time) {
    tries++;
    if (tries >= 8) {
        EndStep();
        return;
    }
    int idp = pack_id;
    while (idp < packs.size()) {
        // Determine the pack to be sent
        if (packs[idp].empty()) {
            // In this case, the pack meant to be sent is not in this PE anymore
            ++idp;
            continue;
        }
        // Determine a semi_random receiving end, based on gossip
        int rand_rec = FindReceiver();
        
        // Send pack to receiver, incrementing the acks needed
        acks_needed++;
        thisProxy[rand_rec].PackAck(idp, CkMyPe(), packs[idp].size(), false);
        
        if (one_time) {
            // If the pack is being resent, the function will stop
            break;
        }
        ++idp;
    }
}

void PackDropLB::PackAck(int id, int from, int psize, bool force) {
    // Sends back the info, with the apropriate bool related to wheter it's accepted or not
    // If the migration is forced, it will be accepted.
    bool ack = ((my_load + pack_load*psize < avg_load*(1+threshold)) || force);
    if (ack) {
        migrates_expected+=psize;
        my_load += pack_load*psize;
    }
    thisProxy[from].RecvAck(id, CkMyPe(), ack);
}

void PackDropLB::RecvAck(int id, int to, bool success) {
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
            //CkPrintf("[%d] Creating migration message of size %d:\n", CkMyPe(), total_migrates);
            msg = new(total_migrates, CkNumPes(), CkNumPes(), 0) LBMigrateMsg;
            msg->n_moves = total_migrates;
            //CkPrintf("[%d] Creating migration message:\n", CkMyPe());
            for (size_t i = 0; i < total_migrates; ++i) {
                MigrateInfo* inf = (MigrateInfo*) migrateInfo[i];
                msg->moves[i] = *inf;
                delete inf;
            }
            migrateInfo.clear();
            lb_end = true;
            //CkPrintf("[%d] I have contributed\n", CkMyPe());
            contribute(CkCallback(CkReductionTarget(PackDropLB, Final_Barrier), thisProxy));
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

void PackDropLB::ForcedPackSend(int id, bool force) {
    //if (force) CkPrintf("[%d] Forcely balancing load\n", CkMyPe());
    int rand_rec = FindReceiver();
    //CkPrintf("[%d] Chosen receiver was %d\n", CkMyPe(), rand_rec);
    tries++;
    acks_needed++;
    thisProxy[rand_rec].PackAck(id, CkMyPe(), packs.at(id).size(), force);
}

void PackDropLB::EndStep() {
    if (total_migrates < pack_count && tries < 8) {
        CkPrintf("[%d] Gotta migrate more: %d\n", CkMyPe(), tries);
        PackSend();
    } else {
        msg = new(total_migrates, CkNumPes(), CkNumPes(), 0) LBMigrateMsg;
        msg->n_moves = total_migrates;
        //CkPrintf("[%d] Creating migration message:\n", CkMyPe());
        for (size_t i = 0; i < total_migrates; ++i) {
            MigrateInfo* inf = (MigrateInfo*) migrateInfo[i];
            //std::memcpy(msg->moves+i, inf, sizeof(MigrateInfo));
            msg->moves[i] = *inf;
            //CkPrintf(":%d:", inf->from_pe);
            delete inf;
        }
        migrateInfo.clear();
        lb_end = true;
        contribute(CkCallback(CkReductionTarget(PackDropLB, Final_Barrier), thisProxy));
    }
}

void PackDropLB::Final_Barrier() {
    //CkPrintf("[%d] This core has reach the end of program\n", CkMyPe());
    ProcessMigrationDecision(msg);
}

/*
* Gossip load information between peers. Receive the gossip message.
*/
void PackDropLB::GossipLoadInfo(int req_h, int from_pe, int n,
    int remote_pe_no[], double remote_loads[]) {
  // Placeholder temp vectors for the sorted pe and their load 
  std::vector<int> p_no;
  std::vector<double> l;

  int i = 0;
  int j = 0;
  int m = pe_no.size();

  // Merge (using merge sort) information received with the information at hand
  // Since the initial list is sorted, the merging is linear in the size of the
  // list. 
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

  // After the merge sort, swap. Now pe_no and loads have updated information
  pe_no.swap(p_no);
  loads.swap(l);
  req_hop = req_h + 1;

  SendLoadInfo();
}

/*
* Construct the gossip message and send to peers
*/
void PackDropLB::SendLoadInfo() {
  // TODO: Keep it 0.8*log
  // This PE has already sent the Randimum set threshold for gossip messages.
  // Hence don't send out any more messages. This is to prevent flooding.
  if (gossip_msg_count > kMaxGossipMsgCount) {
    return;
  }

  // Pick two random neighbors to send the message to
  int rand_nbor1;
  int rand_nbor2 = -1;
  do {
    rand_nbor1 = rand() % CkNumPes();
  } while (rand_nbor1 == CkMyPe());
  // Pick the second neighbor which is not the same as the first one.
  do {
    rand_nbor2 = rand() % CkNumPes();
  } while ((rand_nbor2 == CkMyPe()) || (rand_nbor2 == rand_nbor1));

  // kPartialInfoCount indicates how much information is send in gossip. If it
  // is set to -1, it means use all the information available.
  int info_count = (kPartialInfoCount >= 0) ? kPartialInfoCount : pe_no.size();
  int* p = new int[info_count];
  double* l = new double[info_count];
  for (int i = 0; i < info_count; i++) {
    p[i] = pe_no[i];
    l[i] = loads[i];
  }

  thisProxy[rand_nbor1].GossipLoadInfo(req_hop, CkMyPe(), info_count, p, l);
  thisProxy[rand_nbor2].GossipLoadInfo(req_hop, CkMyPe(), info_count, p, l);

  // Increment the outgoind msg count
  gossip_msg_count++;

  delete[] p;
  delete[] l;
}

#include "PackDropLB.def.h"
