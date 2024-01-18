#ifndef DBSYNC2_H
#define DBSYNC2_H

#include <vector>
#include <string>
#include <memory>
#include <string.h>
#include <assert.h>

#include "log/log.h"
#include "dbsync_engine.h"

namespace Dbsync2
{

struct Sync : public Dbsync
{
    using Dbsync::Dbsync;

    void add_sync_object(std::unique_ptr<Dbsync_object> &&object);
};

enum class Field_type : int
{
    none,
    integer,
    string,
    // вводим тип чтобы избежать путаницы с кодировками в именах ориджинов
    string_blob,
    blob,
    date_time,
    date,
};

enum class Field_flag : int
{
    none,
    // поле только читается, в записи не участвует
    read_only = 0x01,
};

std::underlying_type<Field_flag>::type operator &(Field_flag l, Field_flag r);
Field_flag operator |(Field_flag l, Field_flag r);

struct Join
{
    Join(const char *field_name, int object_type, const char *object_field_name)
        : field_name{field_name}, object_type{object_type}, object_field_name{object_field_name} {}
    const char *field_name;
    int object_type;
    const char *object_field_name;
    const char *object_name = nullptr;
};

struct Field
{
    virtual ~Field()
    {
        clear();
    }
    virtual void operator =(const Field &rhs) = 0;
    virtual void clear() {};

    const char *field_name;
    Field_type field_type;
    Field_flag flags = Field_flag::none;
    std::shared_ptr<Join> join = nullptr;
protected:  // конструкторы закрыты, чтобы обеспечить работы с полями только через shared_ptr
    friend class Fields_container;
    explicit Field(const char *name, Field_type type, Field_flag flags = Field_flag::none)
        : field_name{name}, field_type{type}, flags{flags} {}
    explicit Field(const char *name, Field_type type, std::shared_ptr<Join> &join)
        : field_name{name}, field_type{type}, flags{Field_flag::read_only}, join{join} {}
};

struct Field_integer : public Field
{
    // внешние данные
    int &ref;
    // свои данные, если нет внешних
    int value;

    void operator =(const Field &rhs)
    {
        ref = dynamic_cast<const Field_integer&>(rhs).ref;
    }
    void operator =(int rhs)
    {
        ref = rhs;
    }
    void clear()
    {
        ref = 0;
    }
protected:
    friend class Fields_container;
    Field_integer(const char *name)
        : Field_integer(name, value) {}
    template<typename ... Args>
    Field_integer(const char *name, int &value, Args&& ... args)
        : Field(name, Field_type::integer, std::forward<Args>(args) ...), ref{value} {}
};

struct Field_string : public Field
{
    void operator =(const Field &rhs)
    {
        delete ref;
        ref = strdup(dynamic_cast<const Field_string&>(rhs).ref);
    }
    void operator =(const char *rhs)
    {
        delete ref;
        ref = strdup(rhs);
    }
    void clear()
    {
        delete ref;
        ref = nullptr;
    }
    char *&ref;
    char *value;

protected:
    friend class Fields_container;
    Field_string(const char *name)
        : Field_string(name, value) {}
    template<typename ... Args>
    Field_string(const char *name, char *&value, Args&& ... args)
        : Field(name, Field_type::string, std::forward<Args>(args) ...), ref{value}
    {
        ref = nullptr;
    }
};

struct Field_string_blob : public Field_string
{
    using Field_string::operator =;
protected:
    friend class Fields_container;
    Field_string_blob(const char *name)
        : Field_string_blob(name, value) {}
    template<typename ... Args>
    Field_string_blob(const char *name, char* &value, Args&& ... args)
        : Field_string(name, value, std::forward<Args>(args) ...)
    {
        field_type = Field_type::string_blob;
    }
};

struct Field_blob : public Field
{
    void operator =(const Field &rhs)
    {
        init(dynamic_cast<const Field_blob&>(rhs).ref, dynamic_cast<const Field_blob&>(rhs).length);
    }
    void from_array(const unsigned char* data, int len)
    {
        init(data, len);
    }
    void clear()
    {
        delete ref;
        ref = nullptr;
        length = 0;
    }
    unsigned char* &ref;
    int &length;
protected:
    friend class Fields_container;
    template<typename ... Args>
    Field_blob(const char *name, unsigned char* &value, int &length, Args&& ... args)
        : Field(name, Field_type::blob, std::forward<Args>(args) ...), ref{value}, length{length}
    {
        ref = nullptr;
        length = 0;
    }
private:
    void init(const unsigned char* data, int len)
    {
        delete ref;
        ref = new unsigned char[len];
        memcpy(ref, data, len);
        length = len;
    }
};

struct Field_date_time : public Field
{
    void from_values(int day, int month, int year, int hour, int minute, int second)
    {
        clear();
        ref.tm_mday = day;
        ref.tm_mon = month;
        ref.tm_year = year;
        ref.tm_hour = hour;
        ref.tm_min = minute;
        ref.tm_sec = second;
    }
    void clear()
    {
        memset(&ref, 0, sizeof(ref));
    }
    struct tm &ref;
    struct tm value;
protected:
    friend class Fields_container;
    template<typename ... Args>
    Field_date_time(const char *name, struct tm &value, Args&& ... args)
        : Field(name, Field_type::date_time, std::forward<Args>(args) ...), ref(value) {}

    void operator =(const Field &rhs)
    {
        ref = dynamic_cast<const Field_date_time&>(rhs).ref;
    }
};


struct Field_date : public Field_date_time
{
    void from_values(int day, int month, int year)
    {
        clear();
        ref.tm_mday = day;
        ref.tm_mon = month;
        ref.tm_year = year;
    }
protected:
    friend class Fields_container;
    template<typename ... Args>
    Field_date(const char *name, struct tm &value, Args&& ... args)
        : Field_date_time(name, value, std::forward<Args>(args) ...)
    {
        field_type = Field_type::date;
    }
};

template<class FIELD>
decltype(FIELD::ref) & ref(const std::shared_ptr<Field> &field)
{
    return dynamic_cast<FIELD*>(field.get())->ref;
}

template<class FIELD>
decltype(FIELD::length) & ref_length(const std::shared_ptr<Field> &field)
{
    return static_cast<FIELD*>(field.get())->length;
}

template<class FIELD>
FIELD & field(const std::shared_ptr<Field> &field)
{
    return *dynamic_cast<FIELD*>(field.get());
}

class Fields_container
{
public:
    Fields_container() = default;
    Fields_container(const char *name) : m_name{name} {}
    virtual ~Fields_container() = default;

    template<class FIELD>
    using Item = std::shared_ptr<FIELD>;

    using Vector = std::vector<Item<Field>>;

    Vector & fields()
    {
        return m_fields;
    }
    const Vector & fields() const
    {
        return m_fields;
    }
    const std::string & fields_list() const
    {
        return m_fields_list;
    }
    const char *name() const
    {
        return m_name;
    }

    void add_fields(const Vector & fields)
    {
        if(!m_fields_list.empty())
            m_fields_list += ",";
        auto it = std::begin(fields);
        for(;;) {
            m_fields.push_back(*it);
            m_fields_list += (*it)->field_name;
            ++it;
            if(it == std::end(fields))
                break;
            m_fields_list += ",";
        }
    }

    template<class FIELD, class... ARGS>
    std::shared_ptr<FIELD> make_field_ref(const char *name, decltype(FIELD::ref) value, ARGS&&... args)
    {
        return std::shared_ptr<FIELD>(new FIELD(name, value, std::forward<ARGS>(args)...));
    }

    template<class FIELD>
    std::shared_ptr<FIELD> make_field(const char *name, const char *value)
    {
        std::shared_ptr<FIELD> f(new FIELD(name));
        // инициализируем через приравнивание, чтобы правильно скопировал
        *f = value;
        return f;
    }

protected:
    void clear_fields()
    {
        m_fields_list.clear();
        m_fields.clear();
    }
    void copy_field_values(const std::vector<std::shared_ptr<Field>> & from)
    {
        assert(m_fields.size() == from.size());

        for(unsigned int i = 0; i < m_fields.size(); i++)
            *m_fields[i] = *from[i];
    }

protected:
    const char *m_name = nullptr;

private:
    std::string m_fields_list;
    std::vector<std::shared_ptr<Field>> m_fields;
};

class Query : public Fields_container
{
    std::vector<const Fields_container*> m_tables;
    std::vector<const Fields_container*> m_filters;

public:
    const Fields_container &table() const
    {
        return *m_tables[0];
    }
    const std::vector<const Fields_container*> & filters() const
    {
        return m_filters;
    }
    void add_table(const Fields_container *table)
    {
        m_tables.push_back(table);
        m_name = table->name();
    }
    void add_filter(const Fields_container *filter)
    {
        m_filters.push_back(filter);
    }
    void clear_filters()
    {
        m_filters.clear();
    }
};

template<class T>
class Object : public T, public Fields_container
{
public:
    Object()
    {
        m_origin = make_field_ref<Field_integer>("nOrigin", T::origin);
        m_sync_number = make_field_ref<Field_integer>("nSyncNumber", T::sync_number);

        add_fields({
                       // делаем текстовым, чтобы можно было читать прям из базы глазами
                       make_field_ref<Field_string>("dtRecordTimestamp", T::record_timestamp),
                       m_sync_number,
                       m_origin
                   });
    }
    int open(int origin, int sync_number) final;
    int next() final;
    int add();
    // общие функции для всех типов, в отличие от Dbsync
    void copy_data(std::unique_ptr<Dbsync_object> &from) final
    {
        // по-другому не выходит
        Object *o2 = dynamic_cast<Object*>(from.get());

        if(o2) {
            copy_field_values(o2->fields());
        } else {
            // объект из dbsync копируем по-старому
            T::copy_data(from);
        }
    }
    void clear_data() final;

protected:
    virtual int proceed_open(const Query &query) = 0;
    virtual int proceed_add(const Query &query) = 0;
    virtual int proceed_next() = 0;
    virtual std::shared_ptr<Join> join() { return nullptr; }

    Query make_add_query();
    int update_add(const Fields_container *filter);

    Item<Field_integer> m_origin;
    Item<Field_integer> m_sync_number;
    Item<Field_string> m_origin_string;
};

class Origin : public Object<Dbsync_origin>
{
public:
    Origin();
    int open(const char *origin_id) final;
    int update(int sync_number) final;
};

class Session : public Object<Dbsync_session>
{
public:
    Session();
};

class Patient : public Object<Dbsync_patient>
{
public:
    Patient();
    int add() final;
};

class Implantation : public Object<Dbsync_implantation>
{
public:
    Implantation();
};

class Log
{
public:
    virtual int write_origin_statistics(int origin, const char *origin_string, const db_sync_statistics &statistics) = 0;
};

}

#endif // DBSYNC2_H
