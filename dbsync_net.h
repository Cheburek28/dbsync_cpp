#ifndef DBSYNC_NET_H
#define DBSYNC_NET_H

#include "tcp_client_server/tcp_client_server_global.h"
#include "log/log.h"
#include "dbsync_engine.h"
#include "dbsync_sqlite.h"

#define NETSYNC_LISTEN_PORT		9302
#define NETSYNC_SERVER_PORT		9303
#define NETSYNC_BROADCAST_QUERY     "query_unisync_servers"

enum
{
    DBNETSYNC_COMMAND_START,
    DBNETSYNC_COMMAND_STOP,
    DBNETSYNC_COMMAND_CANCEL,
    DBNETSYNC_COMMAND_GET_ORIGINS_SNAPSHOT,
    DBNETSYNC_COMMAND_PUT_SYNC_DATA,
    DBNETSYNC_COMMAND_PUT_ORIGINS_SNAPSHOT,
    DBNETSYNC_COMMAND_GET_SYNC_DATA,

    DBNETSYNC_COMMANDS
};


struct NetSyncCallBack : public Dbsync::Callback
{
    virtual ~NetSyncCallBack() {}

    virtual void step(){}
    virtual void started() {}
    virtual void finished(db_sync_statistics) {}
    virtual void unlock_storage() {}
    virtual void lock_storage() {}
};

class DBSyncNetClient : public Dbsync::Callback
{
public:


public:
    DBSyncNetClient(NetSyncCallBack * cb = new NetSyncCallBack());
    virtual ~DBSyncNetClient();

public:
    void send_broadcast();
    void disconnect() {m_netsync_client->disconnect();}
    int select_server(std::string servername); // Connecting to the server with selected host name
    int start_sync_with_remote_device(Dbsync * , std::string tmp_file_dir_path = "", char direction = DB_SYNC_DIRECTION_FROM);
    int buffer_to_file(NetWork_buffer *buffer, char *file);
    std::vector<std::string> get_avaliable_servers();

public:
    void step()
    {
        if (m_callback)
            m_callback->step();
    }

private:
    IClient * m_netsync_client;
    NetSyncCallBack * m_callback;


public:
    struct db_sync_statistics dbsync_statistics;
};

class DBSyncNetServer : public IServer::DataProcessor, public Dbsync::Callback // При создании автоматически создает сервер синхронизации баз данных
{

public:
    DBSyncNetServer(Dbsync *, std::string tmp_file_dir_path = "", NetSyncCallBack * callback = nullptr);
    virtual ~DBSyncNetServer();

public:
    int proceed_data(NetWork_buffer *buffer);
    void client_disconnected(){
        LOG(LOG_INFO, "Client disconnected from server");
        m_callback->finished(m_sync_statistics);
    }

    void step() {
        LOG(LOG_INFO, "Sending command NETSYNC_RESPONSE_STEP");
        NetWork_buffer * nb = new NetWork_buffer();
        nb->add_byte(NETSYNC_RESPONSE_STEP);
        m_netsync_server->send_buffer(nb);
    }

private:
    IServer * m_netsync_server;
    NetSyncCallBack * m_callback;

    Dbsync * m_data_source;
    Dbsync_sqlite * m_tmp_db;
    std::string m_tmp_file_path;
    db_sync_statistics m_sync_statistics;


};

#endif // DBSYNC_NET_H
