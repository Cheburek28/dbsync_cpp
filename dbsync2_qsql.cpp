#include "log/error.h"
#include "dbsync2_qsql.h"
#include <QTextStream>
#include <sstream>
#include <QDebug>
#include <QTextCodec>
#include <QDateTime>

#define SQL_ERROR_DATA(e, sql) \
    ERROR_DATA(Dbsync2_qsql::m_error_code, e.text().toLocal8Bit().data(), e.number(), sql.toLocal8Bit().data())

template<class DBSYNC2_OBJECT>
class Dbsync2_qsql_query : public DBSYNC2_OBJECT
{
public:
    Dbsync2_qsql_query(QSqlDatabase *database, const char *object_name)
        : m_query(*database) { DBSYNC2_OBJECT::m_name = object_name; }
protected:
    QString where_string(const std::vector<const Dbsync2::Fields_container*> filters)
    {
        QString where;

        for(auto filteri = filters.cbegin(); filteri != filters.cend() ;++filteri) {
            for(auto fieldi = (*filteri)->fields().cbegin(); fieldi != (*filteri)->fields().cend(); ++fieldi) {
                if(where.isEmpty())
                    where = " where ";
                where += DBSYNC2_OBJECT::m_name;
                where += ".";
                where += (*fieldi)->field_name;
                where += QString("(?)");
                if(std::next(fieldi) ==  (*filteri)->fields().cend())
                    break;
                where += " and ";
            }
            if(std::next(filteri) == filters.cend())
                break;
            where += " and ";
        }
        return where;
        //where %2.nSyncNumber>(:sync_number) and %2.nOrigin=(:origin)")
    }
    int proceed_open(const Dbsync2::Query &query)
    {
        QString query_str, field_list;
        // пока реализовано только одно объединение
        const Dbsync2::Join* join = nullptr;

        m_query.clear();
        // формируем запрос из полей
        for(auto it = query.fields().cbegin(); ; ++it) {
            if((*it)->join) {
                join = (*it)->join.get();
                field_list += (*it)->join->object_name;
            } else {
                field_list += DBSYNC2_OBJECT::m_name;
            }
            field_list += ".";
            field_list += (*it)->field_name;
            if(std::next(it) == query.fields().end())
                break;
            field_list += ",";
        }

        query_str = QString("select %1 from %2").arg(field_list, query.table().name());

        if(join)
            query_str += QString(" left join %1 on %2.%3=%1.%4").arg(join->object_name, DBSYNC2_OBJECT::m_name, join->field_name, join->object_field_name);

        if(query.filters().size() > 0)
            query_str += where_string(query.filters());

        //qDebug() << query_str;

        if(!m_query.prepare(query_str))
            return SQL_ERROR_DATA(m_query.lastError(), query_str);

        for(auto &filter : query.filters())
            for(auto &field : filter->fields())
                m_query.addBindValue(Dbsync2_qsql::get_field_value(field));

        if(!m_query.exec())
            return SQL_ERROR_DATA(m_query.lastError(), query_str);

        return 0;
    }
    int close() final
    {
        m_query.clear();
        return 0;
    }
    int proceed_next()
    {
        if (m_query.next()) {
            auto & fields = DBSYNC2_OBJECT::fields();

            for(unsigned int i = 0; i < fields.size(); i++) {
                auto & f = fields[i];
                if(Dbsync2_qsql::set_field_value(f, m_query.value(m_query.record().indexOf(f->field_name))) < 0)
                    return -1;
            }

            return DB_SYNC_OK;
        }
        return DB_SYNC_EOF;
    }
    int proceed_add(const Dbsync2::Query &query)
    {
        QString query_str;
        auto &fv = query.fields();
        auto &filters = query.filters();

        if(filters.size() > 0) {
            QString fvl;
            for(auto it = fv.begin(); ;++it) {
                fvl += (*it)->field_name;
                fvl += QString("=(?)");
                if(std::next(it) == fv.end())
                    break;
                fvl += ",";
            }
            query_str = QString("update %1 set %2").arg(query.table().name(), fvl);
            query_str += where_string(filters);
        } else {
            QString fl, vl;
            for(auto it = fv.begin(); ;++it) {
                fl += (*it)->field_name;
                vl += QString("(?)");
                if(std::next(it) == fv.end())
                    break;
                fl += ",";
                vl += ",";
            }
            query_str = QString("insert into %1 (%2) values (%3)").arg(query.table().name(), fl, vl);
        }

        m_query.clear();
        if(!m_query.prepare(query_str))
            return SQL_ERROR_DATA(m_query.lastError(), query_str);

        for(auto &f : fv)
            m_query.addBindValue(Dbsync2_qsql::get_field_value(f));
        for(auto &filter : filters)
            for(auto &f : filter->fields())
                m_query.addBindValue(Dbsync2_qsql::get_field_value(f));

        if(!m_query.exec())
            return SQL_ERROR_DATA(m_query.lastError(), query_str);

        return 0;
    }

    QSqlQuery m_query;
};

using Dbsync2_origin_qsql = Dbsync2_qsql_query<Dbsync2::Origin>;
using Dbsync2_session_qsql = Dbsync2_qsql_query<Dbsync2::Session>;
using Dbsync2_patient_qsql = Dbsync2_qsql_query<Dbsync2::Patient>;
using Dbsync2_implantation_qsql = Dbsync2_qsql_query<Dbsync2::Implantation>;

int Dbsync2_qsql::m_error_code = 0;

void Dbsync2_qsql::error_handler(char *user_buf, int user_len, char *system_buf, int system_len, int, va_list ap)
{
    const char *text = va_arg(ap, const char*);
    int number = va_arg(ap, int);
    const char *sql = va_arg(ap, const char*);

    // текст пользователю
    snprintf(user_buf, user_len, "Database synchronisation error. Please contact the developer.");

    snprintf(system_buf, system_len, "%s (%d), sql: %s", text, number, sql);
}

QVariant Dbsync2_qsql::get_field_value(const std::shared_ptr<Dbsync2::Field> &f)
{
    using namespace Dbsync2;

    switch(f->field_type)
    {
    case Field_type::integer:
        return QVariant(ref<const Field_integer>(f));
    case Field_type::string:
        return QVariant(QString::fromUtf8(ref<const Field_string>(f)));
    case Field_type::string_blob:
    {
        char* value = ref<const Field_string_blob>(f);
        return QVariant(QByteArray::fromRawData(value, strlen(value)));
    }
    case Field_type::blob:
    {
        unsigned char* &v = ref<const Field_blob>(f);
        int &l = ref_length<const Field_blob>(f);
        return QVariant(QByteArray::fromRawData((char*)v, l));
    }
    case Field_type::date_time:
    {
        struct tm &v = ref<const Field_date_time>(f);
        QDateTime d(QDate(v.tm_year, v.tm_mon, v.tm_mday),
                    QTime(v.tm_hour, v.tm_min, v.tm_sec));
        return QVariant(d);
    }
    case Field_type::date:
    {
        struct tm &v = ref<const Field_date>(f);
        QDate d(v.tm_year, v.tm_mon, v.tm_mday);
        return QVariant(d);
    }
    default:
        return QVariant();  // unknown field
    }
}

int Dbsync2_qsql::set_field_value(std::shared_ptr<Dbsync2::Field> &f, const QVariant &value)
{
    using namespace Dbsync2;

    switch(f->field_type)
    {
    case Field_type::integer:
        field<Field_integer>(f) = value.toInt();
        break;
    case Field_type::string:
    {
        QByteArray s = value.toString().toUtf8();
        field<Field_string>(f) = s.data();
        break;
    }
    case Field_type::string_blob:
        field<Field_string_blob>(f) = value.toByteArray().data();
        break;
    case Field_type::blob:
        field<Field_blob>(f).from_array((unsigned char*)value.toByteArray().data(), value.toByteArray().size());
        break;
    case Field_type::date_time:
    {
        QDateTime d = value.toDateTime();
        if(d.isValid()) {
            field<Field_date_time>(f).from_values(d.date().day(), d.date().month(), d.date().year(),
                                                  d.time().hour(), d.time().minute(), d.time().second());
        } else {
            f->clear();
        }
    }
        break;
    case Field_type::date:
    {
        QDate d = value.toDate();
        if(d.isValid())
            field<Field_date>(f).from_values(d.day(), d.month(), d.year());
        else
            f->clear();
    }
        break;
    default:
        return ERROR_TEXT(Dbsync2_qsql::m_error_code, "Unknown field type");
    }

    return 0;
}

Dbsync2_qsql::Dbsync2_qsql(QSqlDatabase &database)
    : Dbsync2::Sync(new Dbsync2_origin_qsql(&database, "tbl_sync_origins")), m_database(database)
{
    m_error_code = error()->add_handler("ERROR_DBSYNC_MYSQL", Dbsync2_qsql::error_handler);

    add_sync_object(std::unique_ptr<Dbsync_object>(new Dbsync2_patient_qsql(&m_database, "tbl_patients")));
    add_sync_object(std::unique_ptr<Dbsync_object>(new Dbsync2_implantation_qsql(&m_database, "tbl_implantations")));
    add_sync_object(std::unique_ptr<Dbsync_object>(new Dbsync2_session_qsql(&m_database, "tbl_sessions")));
}

int Dbsync2_qsql::increment_sync()
{
    if(m_database.driverName() == "QMYSQL") {
        // в случае с MYSQL сервером инкрементить не нужно, т.к. просто собирает данные из других баз
    } else {
        QSqlQuery query(m_database);
        QString query_str = "update tbl_sync_origins set nLastSyncNumber=((select nLastSyncNumber from \
                       tbl_sync_origins where nID=1)+1) where nID=1";

        if(!query.exec(query_str))
                return SQL_ERROR_DATA(query.lastError(), query_str);
    }
    return 0;
}

int Dbsync2_qsql::trans_begin()
{
    if(!m_database.transaction())
        return SQL_ERROR_DATA(m_database.lastError(), QString("transaction"));
    return 0;
}

int Dbsync2_qsql::trans_commit()
{
    if(!m_database.commit())
        return SQL_ERROR_DATA(m_database.lastError(), QString("commit"));
    return 0;
}

int Dbsync2_qsql::trans_rollback()
{
    if(!m_database.rollback())
        return SQL_ERROR_DATA(m_database.lastError(), QString("rollback"));
    return 0;
}

int Dbsync2_qsql::open_source()
{
    if(!m_database.open())
        return SQL_ERROR_DATA(m_database.lastError(), QString("trying to open"));

    return 0;
}

int Dbsync2_qsql::close_source()
{
    origin.reset();
    m_database.close();

    return 0;
}

int Dbsync2_qsql::write_origin_statistics(int origin, const char *origin_string, const db_sync_statistics &statistics)
{
    QSqlQuery query(m_database);
    QString query_str;

    query_str = QString("INSERT INTO tbl_sync_register (origin, origin_name, patient, implantation, session)"
        "VALUES (%1, '%2', %3, %4, %5)").arg(origin).arg(origin_string).
            arg(statistics.objects_added[DB_SYNC_OBJECT_PATIENT]).
            arg(statistics.objects_added[DB_SYNC_OBJECT_IMPLANTATION]).
            arg(statistics.objects_added[DB_SYNC_OBJECT_SESSION]);
    if(!query.exec(query_str))
        return SQL_ERROR_DATA(query.lastError(), query_str);
    return 0;
}
