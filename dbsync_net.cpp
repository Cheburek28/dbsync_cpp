#include "dbsync_net.h"
#include "log/log.h"

#include <string>
#include <cstdio>

#include "dbsync_sqlite.h"

#define DBSYNC_NET_TEMP_FILE_NAME "unisync.db"

DBSyncNetClient::DBSyncNetClient(NetSyncCallBack *cb) :
    m_netsync_client{client(NETSYNC_LISTEN_PORT, NETSYNC_SERVER_PORT)},
    m_callback{cb}
{
//    send_broadcast();
}

DBSyncNetClient::~DBSyncNetClient()
{
    delete m_netsync_client;
}

void DBSyncNetClient::send_broadcast()
{
    m_netsync_client->send_broadcast(std::string(NETSYNC_BROADCAST_QUERY));
}

int DBSyncNetClient::select_server(std::string server_name)
{
    return m_netsync_client->server_selected(server_name);
}

int DBSyncNetClient::start_sync_with_remote_device(Dbsync * client_db, std::string tmp_file_dir_path, char direction)
{    

    m_callback->started();

    //Создаем временную sqlite базу для отправки на сервер
    tmp_file_dir_path.append(DBSYNC_NET_TEMP_FILE_NAME);
    remove(tmp_file_dir_path.c_str()); // Удаляем временный файл, так как он мог использоваться при работе сервера
    Dbsync_sqlite tmp_db(tmp_file_dir_path.c_str());


    NetWork_buffer *buffer = new NetWork_buffer();

    if (client_db->open_source() < 0)
        goto e;

    buffer->reset();
    buffer->add_byte(DBNETSYNC_COMMAND_START);

    LOG(LOG_INFO, "Sending command: NETSYNC_COMMAND_START \n");
    if (m_netsync_client->tcp_send_data(buffer) < 0) {
        LOG(LOG_INFO, "Error with sending %s", (const char *)buffer->get_buffer());
        goto e;
    } else {
        LOG(LOG_INFO, "Received data: %s", (const char *)buffer->get_buffer());
    }

    if (direction & DB_SYNC_DIRECTION_TO)
    {
        buffer->reset();
        buffer->add_byte(DBNETSYNC_COMMAND_GET_ORIGINS_SNAPSHOT);

        LOG(LOG_INFO, "Sending command: DBNETSYNC_COMMAND_GET_ORIGINS_SNAPSHOT \n");
        if (m_netsync_client->tcp_send_data(buffer) < 0) {
            LOG(LOG_INFO, "Error with sending %s", (const char *)buffer->get_buffer());
            goto e;
        } else {
            LOG(LOG_INFO, "Received data: %s", (const char *)buffer->get_buffer());
        }

        buffer->reset_ptr();
        buffer->to_file(tmp_file_dir_path.c_str());

        if (tmp_db.open_source() < 0)
            goto e;

        if (client_db->sync(&tmp_db, DB_SYNC_DIRECTION_TO) < 0)
            goto e;

        this->dbsync_statistics.objects_readed = std::vector<int>(tmp_db.statistics.objects_added.begin(), tmp_db.statistics.objects_added.end());

        tmp_db.statistics = db_sync_statistics();
        client_db->statistics = db_sync_statistics();

        buffer->reset();
        buffer->add_byte(DBNETSYNC_COMMAND_PUT_SYNC_DATA);

        tmp_db.close_source();

        buffer->from_file(tmp_file_dir_path.c_str());

        LOG(LOG_INFO, "Sending command: DBNETSYNC_COMMAND_PUT_SYNC_DATA \n");
        if (m_netsync_client->tcp_send_data(buffer) < 0) {
            LOG(LOG_INFO, "Error with sending %s", (const char *)buffer->get_buffer());
            goto e;
        } else {
            LOG(LOG_INFO, "Received data: %s", (const char *)buffer->get_buffer());
        }


    }

    if (direction & DB_SYNC_DIRECTION_FROM) {
        remove(tmp_file_dir_path.c_str());

        if (tmp_db.open_source() < 0) {
            LOG(LOG_INFO, "unable to open tmp_db");
            goto e;
        }

        // Копируем все ориджины из общей базы и отключаемся
        client_db->copy_origins(&tmp_db);
    //    client_db.close_source();
        tmp_db.close_source();

        LOG(LOG_INFO, "SQLite tmp file closed");

        buffer->reset();
        buffer->add_byte(DBNETSYNC_COMMAND_PUT_ORIGINS_SNAPSHOT);
        if (buffer->from_file(tmp_file_dir_path.c_str())){
            LOG(LOG_INFO, "Error with copying origins file to buffer");
            goto e;
        }

        LOG(LOG_INFO, "Sending command: NETSYNC_COMMAND_PUT_ORIGINS_SNAPSHOT \n");

        if (m_netsync_client->tcp_send_data(buffer) < 0) {
            LOG(LOG_INFO, "Error with sending %s", (const char *)buffer->get_buffer());
            goto e;
        } else {
            LOG(LOG_INFO, "Received data: %s", (const char *)buffer->get_buffer());
        }

        buffer->reset();
        buffer->add_byte(DBNETSYNC_COMMAND_GET_SYNC_DATA);

        LOG(LOG_INFO, "Sending command: NETSYNC_COMMAND_GET_SYNC_DATA \n");

        if (m_netsync_client->tcp_send_data(buffer) < 0) {
            LOG(LOG_INFO, "Error with sending %s", (const char *)buffer->get_buffer());
            goto e;
        } else {
            LOG(LOG_INFO, "Received data: %s", (const char *)buffer->get_buffer());
        }

        // Удаляем врменный файл в котором отправлялись origins
        remove(tmp_file_dir_path.c_str());
        buffer->reset_ptr();
        buffer->to_file(tmp_file_dir_path.c_str());

        if (tmp_db.open_source() < 0) {
            LOG(LOG_INFO, "Unable to open tmp file");
            goto e;
        }

        if (client_db->sync(&tmp_db, DB_SYNC_DIRECTION_FROM) < 0) {
            LOG(LOG_INFO, "Can't sync to tmp database.");
            goto e;
        }

        this->dbsync_statistics.objects_added = std::vector<int>(client_db->statistics.objects_added.begin(), client_db->statistics.objects_added.end());

        tmp_db.statistics = db_sync_statistics();
        client_db->statistics = db_sync_statistics();

        LOG(LOG_INFO, "Sync finished");

        if (tmp_db.close_source() < 0) {
            LOG(LOG_INFO, "Unable to close tmp file");
            goto e;
        }

//        if (remove(tmp_file_dir_path.c_str()) < 0) {
//            LOG(LOG_INFO, "Unable to remove tmp file");
//            goto e;
//        }


    }

    buffer->reset();

    buffer->add_byte(DBNETSYNC_COMMAND_STOP);

    LOG(LOG_INFO, "Sending command: NETSYNC_COMMAND_STOP \n");

    if (m_netsync_client->tcp_send_data(buffer) < 0) {
        LOG(LOG_INFO, "Error with sending %s", (const char *)buffer->get_buffer());
        goto e;
    } else {
        LOG(LOG_INFO, "Received data: %s", (const char *)buffer->get_buffer());
    }

    client_db->close_source();

    LOG(LOG_INFO, "Statistics - patients: %d, implantations: %d, sessions: %d", this->dbsync_statistics.objects_added[2], this->dbsync_statistics.objects_added[3], this->dbsync_statistics.objects_added[4]);

    m_callback->finished(this->dbsync_statistics);
    m_netsync_client->disconnect();

    return 0;

e:


    buffer->reset();
    buffer->add_byte(DBNETSYNC_COMMAND_CANCEL);

    LOG(LOG_INFO, "Sending command: DBNETSYNC_COMMAND_CANCEL \n");

    if (m_netsync_client->tcp_send_data(buffer) < 0) {
        LOG(LOG_INFO, "Error with sending %s", (const char *)buffer->get_buffer());
    } else {
        LOG(LOG_INFO, "Received data: %s", (const char *)buffer->get_buffer());
    }

    client_db->close_source();
//    remove(tmp_file_dir_path.c_str());

    m_callback->finished(this->dbsync_statistics);
    m_netsync_client->disconnect();

    return -1;
}

std::vector<std::string> DBSyncNetClient::get_avaliable_servers()
{
    std::vector<std::string> v = m_netsync_client->get_avaliable_servers();
    return v;
}

DBSyncNetServer::DBSyncNetServer(Dbsync *data_source, std::string tmp_file_dir_path, NetSyncCallBack * callback) :
    m_netsync_server{server(NETSYNC_LISTEN_PORT, NETSYNC_SERVER_PORT, std::string(NETSYNC_BROADCAST_QUERY), this)},
    m_callback{callback},
    m_data_source{data_source},
    m_tmp_db{nullptr}
{
    tmp_file_dir_path.append(DBSYNC_NET_TEMP_FILE_NAME);
    m_tmp_file_path = tmp_file_dir_path;
    m_data_source->set_callback(this);
    m_tmp_db = new Dbsync_sqlite(m_tmp_file_path.c_str());
}

DBSyncNetServer::~DBSyncNetServer()
{
    delete m_netsync_server;
}

int DBSyncNetServer::proceed_data(NetWork_buffer *buffer)
{
    char command;
    int rc = -1;

    buffer->get_byte(&command);

    switch(command) {
    case DBNETSYNC_COMMAND_START:
        std::remove(m_tmp_file_path.c_str());
        LOG(LOG_INFO, "Recevied command DBNETSYNC_COMMAND_START");
        if (m_callback)
            m_callback->started();

//        if (m_tmp_db->open_source() < 0)
//            return -1;
//        if (m_data_source->open_source() < 0)
//            return -1;

        buffer->add_result(0);
        return 0;		// continue
    case DBNETSYNC_COMMAND_GET_ORIGINS_SNAPSHOT:
        buffer->add_result(0);

        LOG(LOG_INFO, "Recevied command DBNETSYNC_COMMAND_GET_ORIGINS_SNAPSHOT");

        if (m_data_source->open_source() < 0)
            return -1;

        if (m_tmp_db->open_source() < 0)
            return -1;

        m_data_source->copy_origins(m_tmp_db);

        m_tmp_db->close_source();
        m_data_source->close_source();

        rc = buffer->from_file(m_tmp_file_path.c_str());

        break;
    case DBNETSYNC_COMMAND_PUT_SYNC_DATA:
        std::remove(m_tmp_file_path.c_str()); // Удаляем чтобы выкинуть ориджины
        LOG(LOG_INFO, "Recevied command DBNETSYNC_COMMAND_PUT_SYNC_DATA");
        // load sync data from the client
//        m_tmp_db->close_source();;
        rc = buffer->to_file(m_tmp_file_path.c_str());

        m_callback->unlock_storage();

        if (m_data_source->open_source() < 0)
            return -1;

        if (m_tmp_db->open_source() < 0)
            return -1;

        rc = m_data_source->sync(m_tmp_db, DB_SYNC_DIRECTION_FROM);

        this->m_sync_statistics.objects_added = std::vector<int>(m_tmp_db->statistics.objects_readed.begin(), m_tmp_db->statistics.objects_readed.end());
        m_tmp_db->statistics = db_sync_statistics();
        m_data_source->statistics = db_sync_statistics();
        m_tmp_db->close_source();

        remove(m_tmp_file_path.c_str()); // Удаляем файл с базой клиента

        m_data_source->close_source();

        m_callback->lock_storage();

        buffer->add_result(0);
        break;
    case DBNETSYNC_COMMAND_PUT_ORIGINS_SNAPSHOT:
        LOG(LOG_INFO, "Recevied command DBNETSYNC_COMMAND_PUT_ORIGINS_SNAPSHOT");
        // load snapshot from the client
        rc = buffer->to_file(m_tmp_file_path.c_str());

        if (m_data_source->open_source() < 0)
            return -1;

        if (m_tmp_db->open_source() < 0)
            return -1;

        rc = m_data_source->sync(m_tmp_db, DB_SYNC_DIRECTION_TO);

        this->m_sync_statistics.objects_readed = std::vector<int>(m_tmp_db->statistics.objects_added.begin(),
                                                                 m_tmp_db->statistics.objects_added.end());
        m_tmp_db->statistics = db_sync_statistics();
        m_data_source->statistics = db_sync_statistics();

        m_tmp_db->close_source();
        m_data_source->close_source();

        if (rc == 0)
            buffer->add_result(0);

        break;
    case DBNETSYNC_COMMAND_GET_SYNC_DATA:
        LOG(LOG_INFO, "Recevied command DBNETSYNC_COMMAND_GET_SYNC_DATA");
        buffer->add_result(0);
        rc = buffer->from_file(m_tmp_file_path.c_str());
        remove(m_tmp_file_path.c_str());
        break;
    case DBNETSYNC_COMMAND_STOP:
        LOG(LOG_INFO, "Recevied command DBNETSYNC_COMMAND_STOP");
        m_callback->finished(m_sync_statistics);
        buffer->add_result(0);
        return 1;		// continue
    case DBNETSYNC_COMMAND_CANCEL:
        m_callback->finished(m_sync_statistics);
        buffer->add_result(0);
        return 1;		// continue
    default:
        LOG(LOG_ERROR, "unknown command %d", command);
        rc = -1;
        break;
    }

    if(rc < 0)
        buffer->add_result(rc); // Отправляет NETSYNC_RESPONSE_OK и 4 байта с результатом

    return 0;   //continue

}
