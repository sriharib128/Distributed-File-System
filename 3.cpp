#include <iostream>
#include <fstream> 
#include <vector>
#include <string>
#include <map>
#include <unordered_map>
#include <set>
#include <unordered_set>
#include <queue>
#include <algorithm> 

#include <chrono>
#include <mutex>
#include <thread>
#include <atomic>
#include <mpi.h>

using namespace std;

// For storage server
typedef map<int,vector<char>> id_data; // chunk_id and 32 bytes
typedef unordered_map<string, id_data> file_chunk; // file_name and its chunks

// For metadata server
typedef map<int,vector<int>> id_rank; // chunk_id and its 3 storage nodes // id_rank can also be used as rank_id to store a process and all the chunks ids in it
typedef unordered_map<string, id_rank> file_add; // file name and its chunk_addresses

// to get min_indices in an array
typedef priority_queue<pair<int,int>, vector<pair<int,int>> , greater<pair<int,int>>> min_pq;

int p;
unordered_map<string,int> command_hash;

// UTILITY FUNCTIONS
void send_string(string &file_name, int dest){
    int name_sz = file_name.size(); // sending file name
    MPI_Send(&name_sz,1,MPI_INT,dest,0,MPI_COMM_WORLD);
    MPI_Send(file_name.data(),name_sz,MPI_CHAR,dest,0,MPI_COMM_WORLD);
}
string receive_string(int source){
    int name_sz; // receive file name
    MPI_Recv(&name_sz,1,MPI_INT,source,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
    vector<char> f_n(name_sz);
    MPI_Recv(f_n.data(),name_sz,MPI_CHAR,source,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
    string name(f_n.begin(), f_n.end());
    return name;
}
void send_data(vector<char> & data, int ct, bool from_end, int dest){
    int start_idx =0;
    int data_size = data.size();
    int size = min(ct,data_size);
    if(from_end){
        start_idx = max(data_size-ct,0);
        size = data.size()-start_idx;
    }
    MPI_Send(&size,1,MPI_INT,dest,0,MPI_COMM_WORLD);
    MPI_Send(&data[start_idx],size,MPI_CHAR,dest,0,MPI_COMM_WORLD);
}
vector<char> receive_data(int source){
    int sz;
    MPI_Recv(&sz,1,MPI_INT,source,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
    vector<char> data(sz);
    MPI_Recv(data.data(),sz,MPI_CHAR,source,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
    return data;
}

vector<string> get_cur_input(){
    string s;
    vector<string> cur;
    while(cur.empty()){
        cin>>s;
        if(s=="exit")
            return {"exit"};
        else if(s=="upload"){
            cur.resize(3); cur[0] = s; cin>>cur[1]>>cur[2];
        }
        else if(s =="retrieve"){
            cur.resize(2); cur[0] = s; cin>>cur[1];
        }
        else if(s=="search"){
            cur.resize(3); cur[0] = s; cin>>cur[1]>>cur[2];
        }
        else if(s=="list_file"){
            cur.resize(2); cur[0] = s; cin>>cur[1];
        }
        else if(s=="failover"){
            cur.resize(2); cur[0] = s; 
            int rank; cin>>rank; cur[1] = to_string(rank);
        }
        else if(s=="recover"){
            cur.resize(2); cur[0] = s;
            int rank; cin>>rank; cur[1] = to_string(rank);
        }
        else{
            // cout<<"invalid input"<<endl;
            // cur.push_back("invalid");
            continue;
        }
    }
    return cur;
}

// UPLOAD FUNCTIONS
id_data read_file(const string& file_path) {
    id_data file_id; int chunk_id = 0;

    ifstream file(file_path, ios::binary);
    if (file.is_open()) {
        vector<char> single_chunk(32);

        // Loop to read the file in chunks
        while (file.read(single_chunk.data(), 32)){
            single_chunk.resize(file.gcount());
            file_id[chunk_id++] = single_chunk;
        }   
        //handle remaining data
        if (file.gcount() > 0) {
            single_chunk.resize(file.gcount());
            file_id[chunk_id] = single_chunk;
        }

        file.close();
    }
    return file_id;  // Return the id_data containing all chunks
}

vector<int> get_min_index(vector<int> &p_chunks_ct) {
    min_pq pq;
    for (int i = 1; i < p_chunks_ct.size(); ++i){
        if (p_chunks_ct[i] > 0) // considering only nodes that are online.
            pq.push({p_chunks_ct[i], i});
    }
    vector<int> result;
    for (int i = 0; i<3 && !pq.empty(); ++i){ // get atmost 3 nodes
        result.push_back((pq.top()).second);
        pq.pop();
    }
    return result;
}

vector<int> get_proc_chunks_ct(vector<int> &p_chunks_ct,int ids,bool already_exists){    
    // select 3 nodes for each chunk id to store.
    vector<int> proc_chunk_ct(p,0);
    if(already_exists)
        return proc_chunk_ct;
    vector<int> local_p_chunks_ct = p_chunks_ct;
    vector<int> local_proc_chunk_ct(p,0);
    for(int id=0;id<ids;id++){
        vector<int> top_min_i = get_min_index(local_p_chunks_ct);// select atmost 3 nodes from available to ensure load_balancing
        if(top_min_i.size()==0){
            return proc_chunk_ct;
        }
        for(auto i: top_min_i){
            local_p_chunks_ct[i]++;
            local_proc_chunk_ct[i]++;
        }
    }
    p_chunks_ct = local_p_chunks_ct; // update the global array only if all the chunks are assigned to some nodes.
    proc_chunk_ct = local_proc_chunk_ct;
    /*
    but now each process gets chunks in a random order like 
    process 1 might get chunks 1,3,5 and process 2 might get chunks 2, 4 and 6
    
    but i want to send continuous blocks of chunks like
    process 1 get chunks 1,2,3 and process 2 gets 4,5,6.
    */
    return proc_chunk_ct;
}

id_rank get_file_ranks(vector<int> & p_chunks_ct,int ids,bool already_exists){

    vector<int> proc_chunk_ct = get_proc_chunks_ct(p_chunks_ct,ids,already_exists);
    /* got the final count to send to each process, 
    now using this we can send in continuous blocks.*/
    id_rank file_ranks;
    int ct =0;
    int cur_proc = 1;
    while(cur_proc<p && ct<3){
        for(int id=0;id<ids;id++){
            while(cur_proc<p && proc_chunk_ct[cur_proc]<=0){
                cur_proc++;
            }
            if(cur_proc>=p)
                break;
            file_ranks[id].push_back(cur_proc);
            proc_chunk_ct[cur_proc]--;       
        }
        if(cur_proc>=p)
            break;
        ct++;
    }
    if(ct<1)
        return {};
    return file_ranks;
}

id_rank upload_send_data(string &file_name, string &file_path, vector<int> &p_chunks_ct,bool already_exists){
    
    // read the input file and split into chunks.
    id_data file_data;
    if(!already_exists)
        file_data= read_file(file_path);
    id_rank file_ranks = get_file_ranks(p_chunks_ct,file_data.size(),already_exists);

    // send these all the data assigned to each storage server.
    map<int,vector<int>> proc_chunk_ids;
    for(auto &cur_id_rank:file_ranks){
        int chunk_id = cur_id_rank.first;
        vector<int> storage_ranks = cur_id_rank.second;
        for(auto r:storage_ranks)
            proc_chunk_ids[r].push_back(chunk_id);
    }

    for(int i=1;i<p;i++){
        if(p_chunks_ct[i]<0) continue; // skip down nodes

        int sz = proc_chunk_ids[i].size(); // sending count of chunks
        MPI_Send(&sz,1,MPI_INT,i,0,MPI_COMM_WORLD);
        send_string(file_name,i);// sending file name
        for(auto id:proc_chunk_ids[i]){
            MPI_Send(&id,1,MPI_INT,i,0,MPI_COMM_WORLD); //sending chunk id
            int chunk_size = file_data[id].size();
            MPI_Send(&chunk_size,1,MPI_INT,i,0,MPI_COMM_WORLD);
            MPI_Send(file_data[id].data(),chunk_size,MPI_CHAR,i,0,MPI_COMM_WORLD); // sending chunk
        }
    }
    return file_ranks;
}

void upload_receive_data(file_chunk &storage_server){

    int sz; // receive count off chunks
    MPI_Recv(&sz,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
    string file_name = receive_string(0);// receive file name
    
    for(int i=0;i<sz;i++){
        int id; // receive chunk id
        MPI_Recv(&id,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        int chunk_size;
        MPI_Recv(&chunk_size,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        vector<char> data(chunk_size); // receive chunk
        MPI_Recv(data.data(),chunk_size,MPI_CHAR,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        storage_server[file_name][id] = data; // store chunk in local_memory;
    }
}


// LIST_FILE FUNCTIONS
void list_file_function(file_add &metaserver,string & file_name,vector<int> &p_chunks_ct){
    if(metaserver.find(file_name)==metaserver.end() || metaserver[file_name].size()==0)
        cout<<-1<<endl;
    else{
        for(auto i_r:metaserver[file_name]){
            set<int> s;
            for(auto r: i_r.second){
                if(p_chunks_ct[r]<0) continue;
                s.insert(r);
            }
            //if s.size()==0 break????
            cout<<i_r.first<<" ";
            cout<<s.size()<<" ";
            for(auto r: s)
                cout<<r<<" ";
            cout<<endl;
        }
    }
}


//RETRIEVE FUNCTIONS
int most_available(vector<int> &ranks,vector<int>&p_retrieve_ct){
    set<pair<int,int>> s;
    for(auto i:ranks){
        if(p_retrieve_ct[i]<=0)continue;
        s.insert({p_retrieve_ct[i],i});
    }
    if(s.empty())
        return -1;
    auto it = s.begin();
    return it->second;
}

id_data meta_send_retrieve(vector<int> &p_retreive_ct,string &file_name,unordered_map<int,vector<int>> &proc_chunk_ids){
    id_data retrieved_file;
    for(int i=1;i<p;i++){
        if(p_retreive_ct[i]<0) continue;
        // int send_ct = p_retreive_ct[i]-1;
        int send_ct = proc_chunk_ids[i].size();
        MPI_Send(&send_ct,1,MPI_INT,i,0,MPI_COMM_WORLD);
        // cout<<"proc "<<i<<" - send_ct="<<proc_chunk_ids[i].size()<<endl;
        if(send_ct>0){
            send_string(file_name,i);
            MPI_Send(proc_chunk_ids[i].data(),send_ct,MPI_INT,i,0,MPI_COMM_WORLD);
            for(auto id: proc_chunk_ids[i]){
                int chunk_size;
                MPI_Recv(&chunk_size,1,MPI_INT,i,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                vector<char> data_to_receive(chunk_size);
                MPI_Recv(data_to_receive.data(),chunk_size,MPI_CHAR,i,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                retrieved_file[id] = data_to_receive;
            }     
        }
    }
    return retrieved_file;
}

id_data meta_retrieve_function(vector<int> &p_chunks_ct,file_add& metaserver,string& file_name){
    id_data retrieved_file;
    unordered_map<int,vector<int>> proc_chunk_ids;
    vector<int> p_retreive_ct(p,1);
    for(int i=0;i<p;i++){
        if(p_chunks_ct[i]<0) p_retreive_ct[i]=-1;
    }
    id_rank file = metaserver[file_name];
    for(int id =0;id<file.size();id++){
        int proc = most_available(file[id],p_retreive_ct);
        if(proc==-1){
            for(int i=1;i<p;i++)
                proc_chunk_ids[i]={};
                // p_retreive_ct[i]=-1.; // because we still have to let processes know that we are collecting 0 data from them, else they might be waiting indefinetly.
            break;
        }
        proc_chunk_ids[proc].push_back(id);
        p_retreive_ct[proc]++;
    }
    retrieved_file = meta_send_retrieve(p_retreive_ct,file_name,proc_chunk_ids);
    return retrieved_file;
}

void storage_retrieve_function(file_chunk &storage_server,int my_rank){
    int sz; // receive count off chunks
    MPI_Recv(&sz,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
    vector<int> ids_to_send;
    string file_name;
    if(sz>0){
        ids_to_send.resize(sz);
        file_name = receive_string(0);
        MPI_Recv(ids_to_send.data(),sz,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
    }
    for(auto id: ids_to_send){
        vector<char> data_to_send = storage_server[file_name][id];
        int chunk_size = data_to_send.size();
        MPI_Send(&chunk_size,1,MPI_INT,0,0,MPI_COMM_WORLD);
        MPI_Send(data_to_send.data(),chunk_size,MPI_CHAR,0,0,MPI_COMM_WORLD);
    }
}

// HEARTBEAT FUNCTIONS
int HEARTBEAT_DUR = 250;
int FAILOVER_INTERVAL = 1000;
int HEARTBEAT_TAG  = 1;
atomic<bool> online(true); // each sotarge process checks this to see if it must send hearbeats or not.
mutex manage_proc; // meta server uses this to manage access to last_beat.
vector<int> last_beat; // only meta server can access this to to check last recived heartbeat from each process.
atomic<bool> code_running(true); //each individual proccess has this.. and when false it will stop all the threads of that process for graceful exit.

int get_current_time(){
    auto now = chrono::steady_clock::now();
    int currentTime = chrono::duration_cast<chrono::seconds>(now.time_since_epoch()).count();
    return currentTime;
}

void meta_monitor(vector<bool> &is_online){
    while(code_running.load()){
        { // createing separate scope for lock_guard to ensure mutex release.
            lock_guard<mutex> lock(manage_proc);
            int cur_time = get_current_time();
            for(int i=1;i<p;i++){
                if((cur_time - last_beat[i]) > FAILOVER_INTERVAL/1000){
                    if(is_online[i]){
                        is_online[i] = false;
                        // cout<<"Node "<<i<<" is down"<<endl;
                    }
                }
                else{
                    if(!is_online[i]){
                        is_online[i] = true;
                        // cout<<"Node "<<i<<" is up"<<endl;
                    }
                }
            }
        }
        this_thread::sleep_for(chrono::milliseconds(FAILOVER_INTERVAL/2));
    }
}

void meta_receive_heart_beat(){
    while(code_running.load()){
        int flag,msg;
        MPI_Status status;
        MPI_Iprobe(MPI_ANY_SOURCE,HEARTBEAT_TAG,MPI_COMM_WORLD,&flag,&status);
        if(flag){
            int proc = status.MPI_SOURCE;
            MPI_Recv(&msg,1,MPI_INT,proc,HEARTBEAT_TAG,MPI_COMM_WORLD,&status);
            // cout<<"waiting for lock at 408"<<endl;
            lock_guard<mutex> lock(manage_proc);
            last_beat[proc] = get_current_time(); 
        }
        this_thread::sleep_for(chrono::milliseconds(HEARTBEAT_DUR/13));
    }
}

void storage_send_heart_beat(int my_rank){
    while(code_running.load()){
        this_thread::sleep_for(chrono::milliseconds(HEARTBEAT_DUR));
        if(online.load(memory_order_acquire)){
            // cout<<"----------------sent at "<<get_current_time()<<endl;
            int msg = 1;
            MPI_Request request;
            MPI_Isend(&msg,1,MPI_INT,0,HEARTBEAT_TAG,MPI_COMM_WORLD,&request);
            MPI_Request_free(&request);
        }
    }
}
/* 
- Since we are sending them every second anyway
- Missing an occasional heartbeat is acceptable and 
- Thus we need not use MPI_Wait to ensure completion of the send
- The MPI_Request_free approach might give better performance
*/
// // FAILOVER AND RECOVER FUNCTIONS
void handle_failover_recover(int proc,vector<int> &p_chunks_ct, int hash_cmd){
    if(proc>p || proc<=0)
        cout<<-1<<endl;
    else{
        // lock_guard<mutex> lock(manage_proc);
        // if(hash_cmd == 4)
        //     p_chunks_ct[proc] = -1*abs(p_chunks_ct[proc]);
        // else if(hash_cmd == 5){
        //     last_beat[proc] = get_current_time();
        //     p_chunks_ct[proc] = abs(p_chunks_ct[proc]);
        // }
        cout<<1<<endl;
    }
}

// SEARCH FUNCTIONS
pair< int,pair<int,int> > get_proc_max_block(int id, vector<int> &storage_ranks,map<int,unordered_set<int>> &proc_chunk_ids,vector<int> &p_chunks_ct){
    // get_max_continuous block and corresponding proc
    // each chunk is present on 3 nodes given by storage_ranks and I can retreive from any of them
    // I'm usin proc_chunk_ids(each process and a list of all chunks present in it) to decide from which of the 3 nodes i should retreive
    // i'm doing this by finding choosing which node gives me a max_length continous block.

    int max_ct =0;
    int max_proc = -1;
    for(auto i: storage_ranks){
        if(p_chunks_ct[i]<=0) continue;
        int cur_id = id;
        while(proc_chunk_ids[i].find(cur_id)!=proc_chunk_ids[i].end()){
            cur_id++;
        }
        if(cur_id - id > max_ct){
            max_ct = cur_id -id; max_proc=i;
        }
    }
    return {max_proc,{id,id + max_ct-1}};
}

map<int,vector<pair<int,int>> > get_proc_blocks(id_rank & file_ranks,vector<int> &p_chunks_ct){

    map<int,unordered_set<int>> proc_chunk_ids; 
    for(auto &cur_id_rank:file_ranks){
        int chunk_id = cur_id_rank.first;
        vector<int> storage_ranks = cur_id_rank.second;
        for(auto r:storage_ranks){
            if(p_chunks_ct[r]<=0) continue;
            proc_chunk_ids[r].insert(chunk_id);
        }
    }
    map<int,vector<pair<int,int>> > proc_blocks;
    int id=0;
    while(id<file_ranks.size()){
        pair< int,pair<int,int> > temp = get_proc_max_block(id,file_ranks[id],proc_chunk_ids,p_chunks_ct);
        if(temp.first == -1) // no available node for that chunk
            return {};
        proc_blocks[temp.first].push_back(temp.second);
        id = temp.second.second+1;
    }
    return proc_blocks;
}

set<int> meta_within_chunks(map<int,vector<pair<int,int>> > proc_blocks,vector<int> &p_chunks_ct,string &file_name,string &search_word){
    // get a list of continuous blocks {start_chunk_id, end_chunk_id} for each process
    for(int i=1;i<p;i++){
        if(p_chunks_ct[i]<=0) continue;
        int sz = proc_blocks[i].size();
        MPI_Send(&sz,1,MPI_INT,i,0,MPI_COMM_WORLD);
        if(sz>0){
            send_string(file_name,i);
            send_string(search_word,i);
            for(int j=0;j<sz;j++){
                MPI_Send(&proc_blocks[i][j].first,1,MPI_INT,i,0,MPI_COMM_WORLD);
                MPI_Send(&proc_blocks[i][j].second,1,MPI_INT,i,0,MPI_COMM_WORLD);
            }
        }
    }
    set<int> within_chunk_results;
    for(int i=1;i<p;i++){
        int sz = proc_blocks[i].size();
        if(p_chunks_ct[i]<=0 || sz==0) continue;
        for(int j=0;j<sz;j++){
            int cur_sz;
            MPI_Recv(&cur_sz,1,MPI_INT,i,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            vector<int> offsets(cur_sz);
            if(cur_sz>0)
                MPI_Recv(offsets.data(),cur_sz,MPI_INT,i,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            int block_offset = proc_blocks[i][j].first*32;
            for(auto pos:offsets)
                within_chunk_results.insert(block_offset+pos);
        }
    }
    return within_chunk_results;
}

set<int> meta_between_chunks(map<int,vector<pair<int,int>> > &proc_blocks,vector<int> &p_chunks_ct,string &search_word){
    id_data border_data;// +id=> starting chunk and -id => ending chunk;
    for(auto cur_proc:proc_blocks){
        if(proc_blocks.size()==0)
            continue;
        for(int j=0;j<cur_proc.second.size();j++){ 
            pair<int,int> block = cur_proc.second[j];
            border_data[block.first] = receive_data(cur_proc.first); // we need charting part of block beginning
            border_data[-1*block.second] = receive_data(cur_proc.first); // we need ending part of block ending.
        }
    }
    set<int> results;
    for(auto i:border_data){
        if(i.first>0) continue;
        int cur_chunk_id = abs(i.first);
        int next_chunk_id = cur_chunk_id+1;
        if(border_data.find(next_chunk_id)==border_data.end()) continue;
        
        vector<char> data = i.second;
        int start_offset = next_chunk_id*32 - data.size();

        data.insert(data.end(),border_data[next_chunk_id].begin(),border_data[next_chunk_id].end());
        string dataStr(data.begin(), data.end()); // Convert vector<char> to string
        // cout<<"searching in=="<<dataStr<<"==for=="<<search_word<<endl;
        
        set<string> words_to_search = {
            " " + search_word + " ",
            " " + search_word + "\n",
            "\n" + search_word + " ",
            "\n" + search_word + "\n"
        };
        for(auto word:words_to_search){
            size_t pos = dataStr.find(word);
            while (pos != string::npos) {
                results.insert(pos+ start_offset);
                pos = dataStr.find(word, pos + 1);
            }
        }
    }
    return results;
}

set<int> meta_edge_cases(id_rank&file, vector<int> &p_chunks_ct,string& word,string &file_name){
    int chunk_ct = file.size();
    id_data edge_data;
    set<int> ids_to_search={0};
    if(chunk_ct>1)
        ids_to_search.insert({1,chunk_ct-1,chunk_ct-2});
    unordered_map<int,vector<int>> proc_chunk_ids;
    for(auto id:ids_to_search){
        int proc = -1;
        for(auto r:file[id]){
            if(p_chunks_ct[r]>0){
                proc = r; break;
            }
        }
        if(proc==-1){
            proc_chunk_ids= {};
            break;
        }
        proc_chunk_ids[proc].push_back(id);
    }
    //print proc_chunk_ids
    edge_data = meta_send_retrieve(p_chunks_ct,file_name,proc_chunk_ids);
    if(edge_data.empty())
        return {};
    string starting(edge_data[0].begin(),edge_data[0].end());
    string ending(edge_data[chunk_ct-1].begin(),edge_data[chunk_ct-1].end());
    if(chunk_ct>1){
        string temp(edge_data[1].begin(),edge_data[1].end());
        starting = starting + temp;
        string temp2(edge_data[chunk_ct-2].begin(),edge_data[chunk_ct-2].end());
        ending = temp2+ending;
    }
    // Search for 'word' in 'starting' and 'ending'
    size_t start_pos = starting.find(word);
    size_t end_pos = ending.find(word);
    int len = word.size();

    set<int> found_positions;
    // Check if word is the first word of 'starting'
    if (start_pos == 0 && (starting.size() == len || (starting[len] == ' ' || starting[len] == '\n' ))) {
        found_positions.insert(start_pos-1);
    }

    // Check if word is the last word of 'ending'
    int ending_offset = (chunk_ct-1)*32;
    if(chunk_ct>1)
        ending_offset = (chunk_ct-2)*32;
    while(end_pos!=string::npos){
        if (end_pos + len == ending.size() && (end_pos == 0 || (ending[end_pos - 1] == ' '||ending[end_pos - 1] == '\n'))) 
            found_positions.insert(end_pos -1 + ending_offset);
        end_pos = ending.find(word, end_pos + 1);
    }
    
    return found_positions;
}

void meta_search_function(id_rank& file,string &word,string &file_name,vector<int> p_chunks_ct){

    set<int> edge_cases = meta_edge_cases(file,p_chunks_ct,word,file_name);

    map<int,vector<pair<int,int>> > proc_blocks = get_proc_blocks(file,p_chunks_ct);

    // string search_word = " " + word + " ";
    string search_word = word;
    set<int> within_chunk_results = meta_within_chunks(proc_blocks,p_chunks_ct,file_name,search_word);
    if(proc_blocks.empty()){
        cout<<-1<<endl;
        return;
    }
    set<int> boundary_search_results = meta_between_chunks(proc_blocks,p_chunks_ct,search_word);
    
    within_chunk_results.insert(boundary_search_results.begin(),boundary_search_results.end());
    within_chunk_results.insert(edge_cases.begin(),edge_cases.end());
    cout<<within_chunk_results.size()<<endl;
    for(auto pos:within_chunk_results)
        cout<<pos+1<<" ";
    cout<<endl;
}

vector<vector<int>> storage_search_within_chunks(vector<pair<int,int>> &blocks,id_data& file,string &search_word){

    vector<vector<int>> results(blocks.size());
    for (int block_idx = 0; block_idx < blocks.size(); ++block_idx) {
        auto p = blocks[block_idx];
        string entire_block;
        
        // Concatenate the vectors from the file into a single string
        for (int i = p.first; i <= p.second; ++i)
            entire_block += string(file[i].begin(),file[i].end()); // but file[i] is a vector<char>
        
        // cout<<"searching in=="<<entire_block<<"==for=="<<search_word<<endl;
        // Find all occurrences of search_word in entire_block
        set<string> words_to_search = {
            " " + search_word + " ",
            " " + search_word + "\n",
            "\n" + search_word + " ",
            "\n" + search_word + "\n"
        };
        for(auto word:words_to_search){
            size_t pos = entire_block.find(word);
            while (pos != string::npos) {
                results[block_idx].push_back(pos);
                pos = entire_block.find(word, pos + 1);
            }
        }
        // for(auto word:words_to_search){
        //     int pos = 0;
        //     while ((pos = entire_block.find(search_word, pos)) != string::npos) {
        //         results[block_idx].push_back(pos);
        //         pos += 1;
        //     }
        // }
    }
    return results;
}

void storage_search_function(file_chunk &storage_server,int my_rank){
    int sz; // receive count off blocks
    MPI_Recv(&sz,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
    vector<pair<int,int>> blocks_to_send;
    blocks_to_send.resize(sz);
    string file_name,search_word;
    if(sz==0)
        return ;
    file_name = receive_string(0);
    search_word = receive_string(0);
    for(int j=0;j<sz;j++){
        pair<int,int> block;
        MPI_Recv(&block.first,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        MPI_Recv(&block.second,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        blocks_to_send[j] = block;
    }
    vector<vector<int>> block_offsets = storage_search_within_chunks(blocks_to_send,storage_server[file_name],search_word);
    for(int j=0;j<sz;j++){
        int cur_sz = block_offsets[j].size();
        MPI_Send(&cur_sz,1,MPI_INT,0,0,MPI_COMM_WORLD);
        if(cur_sz>0)
            MPI_Send(block_offsets[j].data(),cur_sz,MPI_INT,0,0,MPI_COMM_WORLD);
    }

    // SENT RESULTS OF SEARCH WITHIN CHUNKS
    // NOW SEND BOUNDARY DATA TO CHECK
    int len = search_word.size();
    for(int j=0;j<sz;j++){
        pair<int,int> chunk = blocks_to_send[j];
        send_data(storage_server[file_name][chunk.first],len-1,false,0);
        send_data(storage_server[file_name][chunk.second],len-1,true,0);
    }

    return ;
}

int main(int argc , char* argv[]){

    // MPI_Init(&argc,&argv);

    int provided;
    // Request MPI_THREAD_MULTIPLE since multiple threads make MPI calls
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    
    // Check if we got the level of thread support we need
    if (provided < MPI_THREAD_MULTIPLE) {
        std::cerr << "ERROR: MPI implementation does not support MPI_THREAD_MULTIPLE\n";
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    int no_proc; MPI_Comm_size(MPI_COMM_WORLD,&no_proc); p = no_proc;
    int my_rank; MPI_Comm_rank(MPI_COMM_WORLD,&my_rank);

    command_hash["upload"]=0 ;command_hash["retrieve"]=1 ;command_hash["search"]=2;
    command_hash["list_file"]=3 ;command_hash["failover"]=4 ;command_hash["recover"]=5;
    command_hash["exit"]=6;
    // vector<vector<string>> input;
    // int no_commands;
    // if(my_rank==0){
    //     input = take_input();
    //     no_commands = input.size();
    // }
    // MPI_Bcast(&no_commands,1,MPI_INT,0,MPI_COMM_WORLD);

    file_add metaserver;
    file_chunk storage_server;
    
    vector<int> p_chunks_ct;
    vector<bool> is_online;
    if(my_rank==0){
        p_chunks_ct.resize(p,1); // to know that node is online
        is_online.resize(p,true);
        p_chunks_ct[0]=0;
        
        // starting to monitor heartbeat
        last_beat.resize(p,0);
        for(int i=1;i<p;i++)
            last_beat[i] = get_current_time();

        thread receive_beat_thread(meta_receive_heart_beat);
        receive_beat_thread.detach();

        thread monitor_beat_thread(meta_monitor,ref(is_online));
        monitor_beat_thread.detach();
    }
    else{
        thread send_beat_thread(storage_send_heart_beat,ref(my_rank));
        send_beat_thread.detach();
    }

    // for(int command = 0;command<no_commands;command++){
    while(true){
        MPI_Barrier(MPI_COMM_WORLD);
        vector<string> cur;int hash_cmd;
        if(my_rank==0){
            cur = get_cur_input();
            if(command_hash.find(cur[0]) == command_hash.end()) continue;
            hash_cmd =command_hash[cur[0]];
            for(int i=1;i<p;i++){
                if(is_online[i])
                    p_chunks_ct[i] = abs(p_chunks_ct[i]);
                else
                    p_chunks_ct[i] = -1*abs(p_chunks_ct[i]);
            }
        }
        MPI_Bcast(&hash_cmd,1,MPI_INT,0,MPI_COMM_WORLD);
        if(hash_cmd==6){
            code_running.store(false);
            break;
        }
        /*
            Implement heartbeat mechanism
            if a node is down then multiply that p_chunks_ct[i] by -1.
            multiply again by -1 when it is up.
        */

    // Failover
       if(hash_cmd == 4){
            int proc_to_fail;
            if(my_rank==0){
                proc_to_fail = stoi(cur[1]);
                handle_failover_recover(proc_to_fail,p_chunks_ct,hash_cmd);
            }
            MPI_Bcast( &proc_to_fail , 1 , MPI_INT , 0 , MPI_COMM_WORLD);
            if(my_rank==proc_to_fail){
                online.store(false,memory_order_release);
                // cout<<"my_rank set online to "<<online.load()<<endl;
            }
            this_thread::sleep_for(std::chrono::seconds(5)); //To make sure next input is delayed.
       }
    // Recover
       else if(hash_cmd == 5){
            int proc_to_recover;
            if(my_rank==0){
                proc_to_recover = stoi(cur[1]);
                handle_failover_recover(proc_to_recover,p_chunks_ct,hash_cmd);
            }
            MPI_Bcast( &proc_to_recover , 1 , MPI_INT , 0 , MPI_COMM_WORLD);
            if(my_rank==proc_to_recover){
                online.store(true,memory_order_release);
            }
            this_thread::sleep_for(std::chrono::seconds(5)); //To make sure next input is delayed.
       }
    // Other functionalities
    // Upload
        else if(hash_cmd == 0){ 
            if(my_rank==0){
                bool already_exists = false;
                if(metaserver.find(cur[1])!=metaserver.end())
                    already_exists = true;
                metaserver[cur[1]] = upload_send_data(cur[1],cur[2],p_chunks_ct,already_exists);
                if(metaserver[cur[1]].size()==0 || already_exists)
                    cout<<-1<<endl;
                else{
                    cout<<1<<endl;
                    list_file_function(metaserver,cur[1],p_chunks_ct);
                }
            }
            else if(online.load())
                upload_receive_data(storage_server);   
        }
    // List_file
        else if(hash_cmd == 3){
            if(my_rank==0)
                list_file_function(metaserver, cur[1], p_chunks_ct);
        }
    // Retrieve
        else if(hash_cmd == 1){
            if(my_rank==0){
                id_data retrieved_file = meta_retrieve_function(p_chunks_ct,metaserver,cur[1]);
                if(retrieved_file.size()==0)
                    cout<<-1<<endl;
                else{
                    for(auto chunk: retrieved_file){
                        for(auto c: chunk.second)
                            cout<<c;
                    }
                    cout<<endl;
                }
            }
            else if(online.load()){
                storage_retrieve_function(storage_server,my_rank);
            }
        }
    // Search
        else if(hash_cmd == 2){
            if(my_rank==0)
                meta_search_function(metaserver[cur[1]],cur[2],cur[1],p_chunks_ct);
            else if(online.load()){
                //SEND first 2 and Last 2 chunks
                storage_retrieve_function(storage_server,my_rank);
                //remaining search.
                storage_search_function(storage_server,my_rank);
            }
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);
    this_thread::sleep_for(std::chrono::milliseconds(2000)); //To make sure all threads of all processes are stopped.
    MPI_Finalize();
    return 0;
}