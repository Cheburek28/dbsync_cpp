#include "log/log.h"
#include "dbsync2.h"

using namespace Dbsync2;

std::underlying_type<Field_flag>::type Dbsync2::operator &(Field_flag l, Field_flag r)
{
    return static_cast<int>(
        static_cast<std::underlying_type<Field_flag>::type>(l) &
        static_cast<std::underlying_type<Field_flag>::type>(r)
    );
}

Field_flag Dbsync2::operator |(Field_flag l, Field_flag r)
{
    return static_cast<Field_flag>(
        static_cast<std::underlying_type<Field_flag>::type>(l) |
        static_cast<std::underlying_type<Field_flag>::type>(r)
    );
}

Origin::Origin()
{
    // у ориджинов особые имена полей
    m_origin = make_field_ref<Field_integer>("nID", origin, Field_flag::read_only);
    m_origin_string = make_field_ref<Field_string_blob>("strID", origin_string);
    m_sync_number = make_field_ref<Field_integer>("nLastSyncNumber", sync_number);

    clear_fields();
    add_fields({ m_origin, m_origin_string, m_sync_number });
}

Session::Session()
{
    add_fields({
                   make_field_ref<Field_integer>("nID", id, Field_flag::read_only),
                   make_field_ref<Field_date_time>("dtSessionDate", date),
                   make_field_ref<Field_integer>("nPacemakerModel", model),
                   make_field_ref<Field_integer>("nPacemakerSerial", serial),
                   make_field_ref<Field_integer>("nSessionType", session_type),
                   make_field_ref<Field_blob>("nSessionData", session_data, length),
               });
}

Patient::Patient()
{
    add_fields({
                   make_field_ref<Field_integer>("nID", id, Field_flag::read_only),
                   make_field_ref<Field_string>("strFirstName", first_name),
                   make_field_ref<Field_string>("strLastName", last_name),
                   make_field_ref<Field_string>("strPatronym", patronym),
                   make_field_ref<Field_string>("strAddress", address),
                   make_field_ref<Field_date>("dtBirthDate", birth_date),
                   make_field_ref<Field_integer>("nSex", sex),
                   make_field_ref<Field_string_blob>("strUniqueID", unique_id),
               });
}

Implantation::Implantation()
{
    std::shared_ptr<Join> patient_join(new Join("nPatientID", DB_SYNC_OBJECT_PATIENT, "nID"));
    add_fields({
                   make_field_ref<Field_integer>("nID", id, Field_flag::read_only),
                   make_field_ref<Field_integer>("nPatientID", patient_id),
                   make_field_ref<Field_integer>("nPacemakerModel", pacemaker_model),
                   make_field_ref<Field_integer>("nPacemakerSerial", pacemaker_serial),
                   make_field_ref<Field_string_blob>("strUniqueID", unique_patient_id, patient_join),
               });
}

int Patient::add()
{
    Fields_container filter;

    filter.add_fields({make_field<Field_string_blob>("strUniqueID=", unique_id)});

    return update_add(&filter);
}

template<class T>
int Object<T>::open(int origin, int sync_number)
{
    Query q;

    Fields_container filter(name());
    Item<Field> o = make_field_ref<Field_integer>("nOrigin=", origin);
    Item<Field> sn = make_field_ref<Field_integer>("nSyncNumber>", sync_number);

    filter.add_fields({o, sn});

    q.add_table(this);
    q.add_fields(fields());
    q.add_filter(&filter);

    return proceed_open(q);
}

template<class T>
int Object<T>::next()
{
    return proceed_next();
}

template<class T>
int Object<T>::add()
{
    Query q = make_add_query();

    return proceed_add(q);
}

template<class T>
void Object<T>::clear_data()
{
    for(auto &f : fields())
        f->clear();
}

template<class T>
Query Object<T>::make_add_query()
{
    Query q;
    Fields_container::Vector fv;
    Fields_container update_filter;

    q.add_table(this);
    for(auto &f : fields()) {
        if(f->flags & Field_flag::read_only)
            continue;
        fv.push_back(f);
    }
    q.add_fields(fv);

    return q;
}

template<class T>
int Object<T>::update_add(const Fields_container *filter)
{
    int rc;
    Query q = make_add_query();

    q.add_filter(filter);

    //сперва определяем, есть ли такая запись
    if(proceed_open(q) < 0)
        return -1;
    rc = proceed_next();
    if(rc < 0)
        return -1;
    if(rc == DB_SYNC_EOF) {
        // записи нет, вставляем новую
        q.clear_filters();
    }

    return proceed_add(q);
}

int Dbsync2::Origin::open(const char *origin_id)
{
    Query q;
    Fields_container filter(name());

    q.add_table(this);
    q.add_fields(fields());

    if(origin_id) {
        filter.add_fields({make_field<Field_string_blob>("strID=", origin_id)});
        q.add_filter(&filter);
    }

    return proceed_open(q);
}

int Origin::update(int sync_number)
{
    Query q;
    Fields_container filter(name());

    Item<Field> o = make_field_ref<Field_integer>("nID=", origin);

    filter.add_fields({o});

    q.add_table(this);
    // инкремент номера синхронизации
    q.add_fields({make_field_ref<Field_integer>("nLastSyncNumber", sync_number)});
    q.add_filter(&filter);

    int rc = proceed_add(q);
    if(rc == 0) {
        Dbsync2::Log *l = dynamic_cast<Dbsync2::Log*>(sync_data);
        if(l)
            l->write_origin_statistics(origin, origin_string, statistics);
    }

    return rc;
}

void Sync::add_sync_object(std::unique_ptr<Dbsync_object> &&object)
{
    // проверяем, нет ли полей с объединениями
    for(auto &f : dynamic_cast<Fields_container*>(object.get())->fields()) {
        Join *j = f->join.get();
        if(j) {
            // если есть, нужно подставить имя объекта
            for(auto &o : objects) {
                if(j->object_type == o->type)
                    j->object_name = dynamic_cast<Fields_container*>(o.get())->name();
            }
            // если вылетает здесь, значит еще не добавлен объект
            // с которым объединение
            assert(j->object_name);
        }
    }

    object->sync_data = this;
    objects.push_back(std::move(object));
}
