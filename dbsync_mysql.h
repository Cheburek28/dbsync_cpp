#ifndef DBSYNC_MYSQL_H
#define DBSYNC_MYSQL_H

#include "dbsync_engine.h"

#include "mysql/jdbc.h"

class Dbsync_mysql : public Dbsync
{
public:
    Dbsync_mysql(const char * host, const char * user, const char * passwd, const char * db_name, Callback * callback = new Callback());
    static int error_code() { return m_error_code; }
    static int step(Dbsync_object *object);
    static int prepare(char *sql, Dbsync_object *object);
    int sync(Dbsync *other, char direction);
    int open_source();
    int close_source();
    int exec(const char *sql);

private:
    static void error_handler(char *user_buf, int user_len, char *system_buf, int system_len, int code, va_list ap);

    sql::Driver *m_driver;
    sql::Connection *m_connection;
    sql::ConnectOptionsMap m_connection_properties;

//    const char* host;
//    const char* user;
//    const char* passwd;
//    const char* db_name;
//    int create_tables(sqlite3 *dbsync);
public:
    static int m_error_code;

    // Dbsync interface
public:

    int increment_sync();
    int trans_begin();
    int trans_commit();
    int trans_rollback();
};

class Dbsync_origin_mysql : public Dbsync_origin
{
    // Dbsync_origin interface
public:
    int open(char *origin_id);
    int update(int sync_number);
    int close();
    int next();
    int add();
};

class Dbsync_implantation_mysql : public Dbsync_implantation
{
public:
    int open(int, int);
    int close();
    int next();
    int add();
};

class Dbsync_patient_mysql : public Dbsync_patient
{
    // Dbsync_origin interface
public:
    int open(int origin, int sync_number);
    int update();
    int close();
    int next();
    int add();
};

class Dbsync_session_mysql : public Dbsync_session
{
    // Dbsync_origin interface
public:
    int open(int origin, int sync_number);
    int update();
    int close();
    int next();
    int add();
};

#endif // DBSYNC_MYSQL_H
