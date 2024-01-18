#ifndef DBSYNC_SQLITE_H
#define DBSYNC_SQLITE_H

#include "dbsync_engine.h"

#include "sqlite3.h"

class Dbsync_sqlite : public Dbsync
{
public:
    Dbsync_sqlite(const char *filename, Callback *callback = new Callback());
    virtual ~Dbsync_sqlite();
    static int error_code() { return m_error_code; }
    static int step(Dbsync_object *object);
    static int prepare(char *sql, Dbsync_object *object);
    int sync(Dbsync *other, char direction);
    int open_source();
    int close_source();

    bool sync_log = false;

private:
    static void error_handler(char *user_buf, int user_len, char *system_buf, int system_len, int code, va_list ap);
    static int m_error_code;
    std::string m_filename;
    char *create_full_filename(const char *dbsyncpath);
    int create_tables(sqlite3 *dbsync);
    int exec(const char *sql);

    // Dbsync interface
public:

    int increment_sync();
    int trans_begin();
    int trans_commit();
    int trans_rollback();
};

class Dbsync_origin_sqlite : public Dbsync_origin
{
    // Dbsync_origin interface
public:
    int open(const char *origin_id);
    int update(int sync_number);
    int close();
    int next();
    int add();
};

class Dbsync_implantation_sqlite : public Dbsync_implantation
{
public:
    int open(int, int);
    int close();
    int next();
    int add();
};

class Dbsync_patient_sqlite : public Dbsync_patient
{
    // Dbsync_origin interface
public:
    int open(int origin, int sync_number);
    int update();
    int close();
    int next();
    int add();
};

class Dbsync_session_sqlite : public Dbsync_session
{   
    // Dbsync_origin interface 
public:
    int open(int origin, int sync_number);
    int update();
    int close();
    int next();
    int add();
};

#endif // DBSYNC_SQLITE_H
