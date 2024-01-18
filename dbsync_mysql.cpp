#include "dbsync_mysql.h"
#include <iostream>
#include <sstream>
#include <assert.h>
#include <vector>
#include <string>

#include "log/error.h"

// disable strdup warning
#pragma warning( disable : 4996 )

int Dbsync_mysql::m_error_code = -1;

Dbsync_mysql::Dbsync_mysql(const char * host, const char * user, const char * passwd, const char * db_name, Callback * callback)
    : Dbsync(new Dbsync_origin_mysql, callback), m_driver{get_driver_instance()}, m_connection{nullptr}
{   
    Dbsync_session_mysql * dbs_s_m = new Dbsync_session_mysql();
    dbs_s_m->sync_data = this;
    objects.insert(objects.begin(), dbs_s_m);
    Dbsync_implantation_mysql * dbs_impl_m = new Dbsync_implantation_mysql();
    dbs_impl_m->sync_data = this;
    objects.insert(objects.begin(), dbs_impl_m);
    Dbsync_patient_mysql * dbs_p_m = new Dbsync_patient_mysql();
    dbs_p_m->sync_data = this;
    objects.insert(objects.begin(), dbs_p_m);

    m_connection_properties["hostName"] = host;
    m_connection_properties["userName"] = user;
    m_connection_properties["password"] = passwd;
    m_connection_properties["schema"] = db_name;
    m_connection_properties["port"] = 3306;
    m_connection_properties["OPT_RECONNECT"] = true;
    m_connection_properties["OPT_CHARSET_NAME"] = "utf8";
    m_connection_properties["OPT_SET_CHARSET_NAME"] = "utf8";

    m_error_code = error()->add_handler("ERROR_DBSYNC_MYSQL", Dbsync_mysql::error_handler);
}

int Dbsync_mysql::step(Dbsync_object *object)
{
    sql::ResultSet *res = (sql::ResultSet*) object->data;

    if (res->next())
        return DB_SYNC_OK;

    if(res->isBeforeFirst())
        return DB_SYNC_ERROR;

    if (res->isAfterLast() || res->isLast())
        return DB_SYNC_EOF;

    return DB_SYNC_ERROR;

}

int Dbsync_mysql::prepare(char *sql, Dbsync_object *object)
{
    sql::Statement *stmt;
    sql::ResultSet *res;

    try {
        stmt = ((sql::Connection*)object->sync_data->data_source)->createStatement();
        res = stmt->executeQuery(sql::SQLString(sql));
        delete stmt;
    } catch (sql::SQLException &e){
        ERROR_DATA(Dbsync_mysql::error_code(), strcat(strdup(e.getSQLStateCStr()), e.what()));
        return -1;
    }

    object->data = res;

    return 0;
}

int Dbsync_mysql::sync(Dbsync *other, char direction)
{
    // в одну сторону
     if(direction & DB_SYNC_DIRECTION_TO) {
         LOG(LOG_INFO, "syncing from local");
         if(other->trans_begin() < 0)
             return -1;
         if (sync_to(other) < 0) {
             other->trans_rollback();
             return -1;
         }
         if(increment_sync() < 0) {
             other->trans_rollback();
             return -1;
         }
         LOG(LOG_INFO, "local sync incremented");
         if(other->trans_commit() < 0)
             return -1;
     }
     // теперь в другую
     if(direction & DB_SYNC_DIRECTION_FROM) {
         LOG(LOG_INFO, "syncing to local");
         if(trans_begin() < 0)
             return -1;
         if (other->sync_to(this) < 0) {
             trans_rollback();
             return -1;
         }
         if(trans_commit() < 0)
             return -1;

     }

     return 0;
}

int Dbsync_mysql::open_source()
{
    try {
        m_connection = m_driver->connect(m_connection_properties);

        sql::Statement *stmt = m_connection->createStatement();
        stmt->execute("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
        stmt->close();
        delete stmt;

        m_connection->setAutoCommit(false);
//        m_connection->setTransactionIsolation(sql::TRANSACTION_READ_UNCOMMITTED);
    } catch (sql::SQLException e){
        ERROR_DATA(Dbsync_mysql::error_code(), strcat(strdup(e.getSQLStateCStr()), e.what()));
        return -1;
    }

    printf("Connected! \n");

    data_source = m_connection;

    return 0;
}

int Dbsync_mysql::close_source()
{
    if (m_connection) {
        m_connection->close();
        delete m_connection;
        m_connection = nullptr;
    }

    data_source = nullptr;

    return 0;
}

void Dbsync_mysql::error_handler(char *user_buf, int user_len, char *system_buf, int system_len, int code, va_list ap)
{
//    Dbsync *sync = va_arg(ap, Dbsync*);
    const char * sql = va_arg(ap, const char*);

    // текст пользователю
    snprintf(user_buf, user_len, "Database synchronisation error. Please contact the developer.");

    snprintf(system_buf, system_len, "Error  :  %s", sql);
}

int Dbsync_mysql::exec(const char *sql)
{

    try {
        sql::Statement *stmt = m_connection->createStatement();
        stmt->executeUpdate(sql);
//        m_connection->commit();
        delete stmt;
        printf("Query executed");
    } catch (sql::SQLException &ex){
        ERROR_DATA(Dbsync_mysql::error_code(), strcat(strdup(sql), ex.what()));
        return -1;
    }

    return 0;
}

int Dbsync_mysql::increment_sync()
{
    const char *sql;

    sql = "update tbl_sync_origins set nLastSyncNumber=nLastSyncNumber+1 where nID=1";

    return this->exec(sql);
}

int Dbsync_mysql::trans_begin()
{
//    try {
//    sql::Statement *stmt = m_connection->createStatement();

//    stmt->execute("ROLLBACK"); // Отключаем автоматически запускающуюся транзакцию при setAutoCommit(false)

//    stmt->execute("START TRANSACTION");

//    stmt->close();
//    delete stmt;
//    } catch (sql::SQLException &ex){
//        ERROR_DATA(Dbsync_mysql::error_code(), ex.what());
//        return -1;
//    }

    return 0;
}

int Dbsync_mysql::trans_commit()
{
//    m_connection->commit();
    try {
    sql::Statement *stmt = m_connection->createStatement();

    stmt->execute("COMMIT");

    stmt->close();
    delete stmt;
    } catch (sql::SQLException &ex) {
        ERROR_DATA(Dbsync_mysql::error_code(), ex.what());
        return -1;
    }

    return 0;
}

int Dbsync_mysql::trans_rollback()
{
//    m_connection->rollback();
    try {
    sql::Statement *stmt = m_connection->createStatement();

    stmt->execute("ROLLBACK");

    stmt->close();
    } catch (sql::SQLException &ex) {
        ERROR_DATA(Dbsync_mysql::error_code(), ex.what());
        return -1;
    }

    return 0;
}

//void Dbsync_mysql::create_table()
//{
//    printf("starting");
//    sql::Driver *driver;
//    sql::Connection *con;
//    sql::Statement *stmt;
//    sql::PreparedStatement *pstmt;

//    try
//    {
//        driver = get_driver_instance();
//        con = driver->connect("controldb.local", "unidb", "elestimcardio");
//    } catch (sql::SQLException e){
//        printf("Could not connect to server. Error - ");
//        printf(e.getSQLStateCStr());
//        printf("/n");
//    }

//    con->setSchema("unidb");
//    stmt = con->createStatement();
//    stmt->execute("CREATE TABLE IF NOT EXISTS test_drive (id serial PRIMARY KEY)");
//    delete stmt;

//    con->close();
//    delete con;
//}

int Dbsync_origin_mysql::open(char *origin_id)
{
    char text[500];

    if (origin_id && (*origin_id))
        sprintf(text, "select nID, strID, nLastSyncNumber from tbl_sync_origins where strID=\"%s\"", origin_id);
    else
        sprintf(text, "select nID, strID, nLastSyncNumber from tbl_sync_origins");

    return Dbsync_mysql::prepare(text, this);
}

int Dbsync_origin_mysql::update(int sync_number)
{
    char text[500];

    sprintf(text, "update tbl_sync_origins set nLastSyncNumber=%d \
                       where nID=%d", sync_number, origin);
    // инкремент номера синхронизаци
    return ((Dbsync_mysql*)sync_data)->exec(text);
}

int Dbsync_origin_mysql::close()
{
    ((sql::ResultSet*) this->data)->close();

    Dbsync_origin::close();

    return 0;
}

int Dbsync_origin_mysql::next()
{
    int rc;

    rc = Dbsync_mysql::step(this);
    if (rc < 0)
        return rc;

    clear_data();
    Dbsync_object::clear_data();

    if (rc == DB_SYNC_EOF)
        return DB_SYNC_EOF;

    origin = ((sql::ResultSet*)data)->getInt("nID");
    origin_string = strdup(((sql::ResultSet*)data)->getString("strID").c_str());
    sync_number = ((sql::ResultSet*)data)->getInt("nLastSyncNumber");

    return DB_SYNC_OK;
}

int Dbsync_origin_mysql::add()
{
    char text[500];
//    int rc;

    sprintf(text, "INSERT INTO tbl_sync_origins (strID, nLastSyncNumber) VALUES (\"%s\", %d)",
            origin_string, sync_number);

    return ((Dbsync_mysql*) sync_data)->exec(text);

//    try {
//        stmt->executeUpdate(text);
//        ((sql::Connection*) this->sync_data->data_source)->commit();
//        delete stmt;
//        printf("Query executed");
//    } catch (...){
//        return -1;
//        char text[100];
//        sprintf(text, "%d ", e.getErrorCode());
//        return ERROR_DATA(Dbsync_mysql::error_code(), strcat(/*strdup(e.getSQLStateCStr())*/text , text));
//    }
}

int Dbsync_implantation_mysql::open(int, int)
{
    char text[1000];

    sprintf(text, "select tbl_implantations.nID, tbl_implantations.nPatientID, \
                   tbl_patients.strUniqueID, \
                   tbl_implantations.nPacemakerModel, tbl_implantations.nPacemakerSerial, \
                   tbl_implantations.dtRecordTimestamp, tbl_implantations.nSyncNumber, \
                   tbl_implantations.nOrigin from tbl_implantations \
                   left join tbl_patients on tbl_implantations.nPatientID=tbl_patients.nID \
                   where tbl_implantations.nSyncNumber>%d and tbl_implantations.nOrigin=%d",
                   sync_number, origin);

    return Dbsync_mysql::prepare(text, this);
}

int Dbsync_implantation_mysql::close()
{
    ((sql::ResultSet*) this->data)->close();

    assert(type == DB_SYNC_OBJECT_IMPLANTATION);

    clear_data();
    Dbsync_object::clear_data();

    return 0;
}

int Dbsync_implantation_mysql::next()
{
    int rc;
    rc = Dbsync_mysql::step(this);
    if (rc < 0)
        return rc;

    clear_data();
    Dbsync_object::clear_data();
    if (rc == DB_SYNC_EOF)
        return DB_SYNC_EOF;

    id = ((sql::ResultSet*)data)->getInt("tbl_implantations.nID");
    patient_id = ((sql::ResultSet*)data)->getInt("tbl_implantations.nPatientID");
    if (patient_id == 516) {
        patient_id = ((sql::ResultSet*)data)->getInt("nID");
    }
    unique_patient_id = strdup(((sql::ResultSet*)data)->getString("tbl_patients.strUniqueID").c_str());
    pacemaker_model = ((sql::ResultSet*)data)->getInt("tbl_implantations.nPacemakerModel");
    pacemaker_serial = ((sql::ResultSet*)data)->getInt("tbl_implantations.nPacemakerSerial");

    record_timestamp = strdup(((sql::ResultSet*)data)->getString("tbl_implantations.dtRecordTimestamp").c_str());
    sync_number = ((sql::ResultSet*)data)->getInt("tbl_implantations.nSyncNumber");
    origin = ((sql::ResultSet*)data)->getInt("tbl_implantations.nOrigin");

    return DB_SYNC_OK;
}

int Dbsync_implantation_mysql::add()
{
    char text[1000];
    int rc;

    // сперва пробуем обновить
    sprintf(text, "select nID from tbl_implantations where \
            nPacemakerModel=%d and nPacemakerSerial=%d",
            pacemaker_model, pacemaker_serial);
    Dbsync_mysql::prepare(text, this);
    rc = Dbsync_mysql::step(this);
    ((sql::ResultSet*) this->data)->close();

    if (rc < 0)
        return rc;
    if (rc == DB_SYNC_OK) {
        sprintf(text, "update tbl_implantations set nPatientID= \
                    (select nID from tbl_patients where strUniqueID=\"%s\"), dtRecordTimestamp=\"%s\", \
                    nSyncNumber=%d, nOrigin=%d where nPacemakerModel=%d and nPacemakerSerial=%d",
                    unique_patient_id, record_timestamp, sync_number, origin,
                    pacemaker_model, pacemaker_serial);
    } else {
        sprintf(text, "insert into tbl_implantations (nPatientID, nPacemakerModel, \
                    nPacemakerSerial, dtRecordTimestamp, nSyncNumber, nOrigin) values \
                    ((select nID from tbl_patients where strUniqueID=\"%s\"), %d, \
                    %d, \"%s\", %d, %d)", unique_patient_id, pacemaker_model, pacemaker_serial,
                    record_timestamp, sync_number, origin);

    }

    return ((Dbsync_mysql*) sync_data)->exec(text);
}

int Dbsync_patient_mysql::open(int origin, int sync_number)
{
    char text[1000];

    sprintf(text, "select tbl_patients.nID, tbl_patients.strFirstName, \
            tbl_patients.strLastName, tbl_patients.strPatronym, tbl_patients.strAddress, \
            tbl_patients.dtBirthDate, tbl_patients.strUniqueID, tbl_patients.dtRecordTimestamp, \
            tbl_patients.nSyncNumber, tbl_patients.nOrigin, tbl_patients.nSex \
            from tbl_patients where tbl_patients.nSyncNumber>%d and tbl_patients.nOrigin=%d", sync_number, origin);

    return Dbsync_mysql::prepare(text, this);
}

int Dbsync_patient_mysql::update()
{
    char date[20];

    sprintf(date, "%04d-%02d-%02d", birth_date.tm_year, birth_date.tm_mon, birth_date.tm_mday);

    char sql[500];
    sprintf(sql, "update tbl_patients set strFirstName=\"%s\", \
                   strLastName=\"%s\", strPatronym=\"%s\", strAddress=\"%s\", \
                   dtBirthDate=\"%s\", dtRecordTimestamp=\"%s\", nSyncNumber=%d, \
                   nOrigin=%d, nSex=%d where strUniqueID=\"%s\"",
        first_name, last_name, patronym,
        address, date, record_timestamp,
        sync_number, origin,
                   sex, unique_id);

    // инкремент номера синхронизации
    return ((Dbsync_mysql*) sync_data)->exec(sql);

}

int Dbsync_patient_mysql::close()
{
    ((sql::ResultSet*) this->data)->close();

    assert(type == DB_SYNC_OBJECT_PATIENT);

    clear_data();
    Dbsync_object::clear_data();

    return 0;
}

int Dbsync_patient_mysql::next()
{
    int rc;
    const char *unique_id;

    rc = Dbsync_mysql::step(this);
    if (rc < 0)
        return rc;

    clear_data();
    Dbsync_object::clear_data();
    if (rc == DB_SYNC_EOF)
        return DB_SYNC_EOF;

    id = ((sql::ResultSet*)data)->getInt("tbl_patients.nID");
    first_name = strdup(((sql::ResultSet*)data)->getString("tbl_patients.strFirstName").c_str());
    last_name = strdup(((sql::ResultSet*)data)->getString("tbl_patients.strLastName").c_str());
    patronym = strdup(((sql::ResultSet*)data)->getString("tbl_patients.strPatronym").c_str());
    address = strdup(((sql::ResultSet*)data)->getString("tbl_patients.strAddress").c_str());
    sscanf(((sql::ResultSet*)data)->getString("tbl_patients.dtBirthDate").c_str(), "%d-%d-%d", &birth_date.tm_year, &birth_date.tm_mon,
           &birth_date.tm_mday);
    unique_id = ((sql::ResultSet*)data)->getString("tbl_patients.strUniqueID").c_str();
    if (unique_id != NULL)
        this->unique_id = strdup(unique_id);

    record_timestamp = strdup(((sql::ResultSet*)data)->getString("tbl_patients.dtRecordTimestamp").c_str());
    sync_number = ((sql::ResultSet*)data)->getInt("tbl_patients.nSyncNumber");
    origin = ((sql::ResultSet*)data)->getInt("tbl_patients.nOrigin");
    sex = ((sql::ResultSet*)data)->getInt("tbl_patients.nSex");

    return DB_SYNC_OK;
}

int Dbsync_patient_mysql::add()
{
    char text[1000];
    int rc;

    // сперва пробуем обновить
    sprintf(text, "select nID from tbl_patients where \
            strUniqueID=\"%s\"", unique_id);
    Dbsync_mysql::prepare(text, this);
    rc = Dbsync_mysql::step(this);
    ((sql::ResultSet*) this->data)->close();

    if (rc < 0)
        return rc;
    if (rc == DB_SYNC_OK) {
        update();

//        rc = ((Dbsync_mysql*) sync_data)->exec(text);
        rc = Dbsync_mysql::prepare(text, this);
        return rc;
    } else {
        char date[20];

        sprintf(date, "%d-%d-%d", birth_date.tm_year, birth_date.tm_mon, birth_date.tm_mday);
        sprintf(text, "insert into tbl_patients (strFirstName, \
                       strLastName, strPatronym, strAddress, dtBirthDate, \
                       dtRecordTimestamp, nSyncNumber, nOrigin, strUniqueID, nSex) \
                values ('%s', '%s', '%s', '%s', '%s', '%s', %d, %d, '%s', %d)",
                first_name, last_name, patronym,
                address, date, record_timestamp,
                sync_number, origin,
                unique_id, sex);

        rc = ((Dbsync_mysql*) sync_data)->exec(text);
        return rc;

    }
    return 0;
}

int Dbsync_session_mysql::open(int origin, int sync_number)
{
    char text[1000];

    sprintf(text, "select tbl_sessions.nID, tbl_sessions.nPacemakerModel, \
                        tbl_sessions.nPacemakerSerial, \
                        tbl_sessions.nSessionType, tbl_sessions.nSessionData, \
                        tbl_sessions.dtSessionDate, \
                        tbl_sessions.dtRecordTimestamp, \
                        tbl_sessions.nSyncNumber, tbl_sessions.nOrigin \
                        from tbl_sessions where tbl_sessions.nSyncNumber>%d and tbl_sessions.nOrigin=%d",
            sync_number, origin);
    return Dbsync_mysql::prepare(text, this);
}

int Dbsync_session_mysql::update()
{
    return 0;
}

int Dbsync_session_mysql::close()
{
    ((sql::ResultSet*) this->data)->close();

    assert(type == DB_SYNC_OBJECT_SESSION);

    clear_data();
    Dbsync_object::clear_data();

    return 0;
}

int Dbsync_session_mysql::next()
{
    int rc;
    std::string sdata;

    rc = Dbsync_mysql::step(this);
    if (rc < 0)
        return rc;

    clear_data();
    Dbsync_object::clear_data();
    if (rc == DB_SYNC_EOF)
        return DB_SYNC_EOF;

    id = ((sql::ResultSet*)data)->getInt("tbl_sessions.nID");
    model = ((sql::ResultSet*)data)->getInt("tbl_sessions.nPacemakerModel");
    serial = ((sql::ResultSet*)data)->getInt("tbl_sessions.nPacemakerSerial");
    session_type = ((sql::ResultSet*)data)->getInt("tbl_sessions.nSessionType");
    sdata = ((sql::ResultSet*)data)->getString("tbl_sessions.nSessionData").asStdString();


    session_data = (unsigned char *)strdup((sdata.c_str()));
    length = sdata.size();
//    memcpy(session_data, data_vect.data(), data_vect.size());
    sscanf(((sql::ResultSet*)data)->getString("tbl_sessions.dtSessionDate").c_str(), "%d-%d-%d %d:%d:%d", &date.tm_year, &date.tm_mon,
           &date.tm_mday, &date.tm_hour, &date.tm_min, &date.tm_sec);

    record_timestamp = strdup(((sql::ResultSet*)data)->getString("tbl_sessions.dtRecordTimestamp").c_str());
    sync_number = ((sql::ResultSet*)data)->getInt("tbl_sessions.nSyncNumber");
    origin = ((sql::ResultSet*)data)->getInt("tbl_sessions.nOrigin");

    return DB_SYNC_OK;
}

int Dbsync_session_mysql::add()
{
    char text[1000];

    char date[20];
    sprintf(date, "%04d-%02d-%02d %02d:%02d:%02d", this->date.tm_year, this->date.tm_mon, this->date.tm_mday,
        this->date.tm_hour, this->date.tm_min, this->date.tm_sec);
    sprintf(text, "insert into tbl_sessions (nPacemakerModel, \
                   nPacemakerSerial, nSessionType, \
                   nSessionData, dtSessionDate, dtRecordTimestamp, \
                   nSyncNumber, nOrigin) values \
                   (%d, %d, %d, ?, \"%s\", \"%s\", %d, %d)",
        model, serial, session_type,
        date, record_timestamp,
        sync_number,
        origin);

    try {
        sql::PreparedStatement * ps = ((sql::Connection*)sync_data->data_source)->prepareStatement(text);
        ps->setString(1, sql::SQLString((const char*)session_data, length));
        ps->executeUpdate();
//        ((sql::Connection*)sync_data->data_source)->commit();
        delete ps;
        printf("Query executed");
    } catch (sql::SQLException &e){
        ERROR_DATA(Dbsync_mysql::error_code(), e.what());
        return -1;
    }

    return 0;
}
