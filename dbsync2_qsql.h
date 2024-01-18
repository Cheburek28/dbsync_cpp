#ifndef DBSYNC2_QSQL_H
#define DBSYNC2_QSQL_H

#include "dbsync2.h"
#include <QSqlDatabase>
#include <QSqlError>
#include <QSqlQuery>
#include <QSqlRecord>
#include <QVariant>

class Dbsync2_qsql : public Dbsync2::Sync, public Dbsync2::Log
{
public:
    static int set_field_value(std::shared_ptr<Dbsync2::Field> &f, const QVariant &value);
    static QVariant get_field_value(const std::shared_ptr<Dbsync2::Field> &f);

    Dbsync2_qsql(QSqlDatabase &database);

    // Dbsync interface
public:
    int increment_sync();
    int trans_begin();
    int trans_commit();
    int trans_rollback();

    int open_source();
    int close_source();

    int write_origin_statistics(int origin, const char *origin_string, const db_sync_statistics &statistics);

    static int m_error_code;

private:
    static void error_handler(char *user_buf, int user_len, char *system_buf, int system_len, int code, va_list ap);

    QSqlDatabase &m_database;
};

#endif // DBSYNC2_QSQL_H
