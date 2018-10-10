#ifndef PACK_DROP_MIG_COST_LB
#define PACK_DROP_MIG_COST_LB

// Charm includes
#include "DistBaseLB.h"
#include "Informed.decl.h"

// Data structures includes
#include "OrderedElement.h"
#include <list>
#include <vector>
#include <queue>
#include <unordered_map>
#include <deque>

// Algorithm includes


// Class definition
void CreateInformed();

class Informed : public CBase_Informed {
 public:
    Informed(const CkLBOptions&);
    Informed(CkMigrateMessage *m);
    void Load_Setup(double total_load);
    void Chare_Setup(int count);
    void PackAck(int pack_id, int from, int psize, bool force);
    void RecvAck(int pack_id, int to, bool success);
    void EndStep();
    void First_Barrier();
    void GossipLoadInfo(int, int, int, int[], double[]);
    void DoneGossip();
    void DetailsRedux(int migrations);
    void Final_Barrier();

 private:
    // Private functions
    void InitLB(const CkLBOptions &);
    void Setup();
    void PackSizeDef();
    void LoadBalance();
    void SendLoadInfo();
    void ShowMigrationDetails();
    void ForcedPackSend(int pack_id, bool force);
    void PackSend(int pack_id = 0, int one_time = 0);
    void Strategy(const DistBaseLB::LDStats* const stats);
    bool QueryBalanceNow(int step) { return true; };
    double RecalculateLoad(int n_recs);
    void CalculateReceivers();
    int FindReceiver();

    // Atributes
    bool lb_end;
    bool lb_started;
    int acks_needed;
    int gossip_msg_count;
    int kMaxGossipMsgCount;
    int kPartialInfoCount;
    double avg_load;
    int chare_count;
    double tries;
    size_t total_migrates; // as in dist_lb
    int my_chares;
    double pack_load;
    double my_load;
    double threshold;
    bool is_receiving;
    int done;
    int pack_count;
    int req_hop;
    int info_send_count;
    int rec_count;
    LBMigrateMsg* msg;
    const DistBaseLB::LDStats* my_stats;
    std::vector<MigrateInfo*> migrateInfo;
    std::list<int> non_receiving_chares;
    std::vector<int> receivers;
    std::vector<int> pe_no;
    std::vector<double> loads;
    std::vector<double> distribution;
    std::priority_queue<Element, std::deque<Element>> local_tasks;
    std::unordered_map<int, std::vector<int>> packs;
    CProxy_InformedPackDropLB thisProxy;


};



#endif /* PACK_DROP_MIG_COST_LB */
