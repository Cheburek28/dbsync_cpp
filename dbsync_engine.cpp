#include "dbsync_engine.h"

#include "log/error.h"
#include <assert.h>

// disable strdup warning
//#pragma warning( disable : 4996 )

/*void db_sync_object_clear_data(db_sync_object_t *object)
{
            object->origin = 0;
            object->sync_number = 0;
            free(object->record_timestamp);
            object->record_timestamp = NULL;
}*/

void Dbsync_object::clear_data()
{
    origin = 0;
    sync_number = 0;
    delete record_timestamp;
    record_timestamp = nullptr;
}

/*void db_sync_origin_clear_data(db_sync_object_t *object)
{
            db_sync_origin_t *origin = (db_sync_origin_t*) object->container;

            free(origin->origin_string);
            origin->origin_string = NULL;
}*/

void Dbsync_origin::clear_data()
{
    Dbsync_object::clear_data();

    delete origin_string;
    origin_string = nullptr;
}

/*int _db_sync(db_sync_t *src, db_sync_t *dst)
{
            int rc_src, rc_dst;

            // список origin в источнике
            rc_src = src->origin->open(src->origin, NULL);
            if (rc_src < 0)
                        return -1;
            rc_src = src->origin->object.next(&src->origin->object);
            if (rc_src < 0)
                        goto e;
            // идем по всем origin источника
            while (rc_src == DB_SYNC_OK) {
                        zlog_log("db_sync", ZLOG_INFO, "source origin %s, sync %d", src->origin->origin_string, src->origin->object.sync_number);
                        // origin в приемнике
                        rc_dst = dst->origin->open(dst->origin, src->origin->origin_string);
                        if (rc_dst < 0)
                                    goto e;
                        rc_dst = dst->origin->object.next(&dst->origin->object);
                        if (rc_dst < 0)
                                    goto e1;
                        if (rc_dst == DB_SYNC_EOF) {
                                    // если такого origin нет, создаем
                                    dst->origin->origin_string = strdup(src->origin->origin_string);
                                    rc_dst = dst->origin->object.add(&dst->origin->object);
                                    if (rc_dst < 0)
                                                goto e1;
                                    zlog_log("db_sync", ZLOG_INFO, "created new origin %s", dst->origin->origin_string);
                                    // переоткрываем, чтобы зацепить ID
                                    dst->origin->object.close(&dst->origin->object);
                                    continue;
                        }
                        zlog_log("db_sync", ZLOG_INFO, "destination origin %s, sync %d", dst->origin->origin_string, dst->origin->object.sync_number);
                        // перемещаем данные
                        rc_dst = _db_sync_origin(src, dst);
                        if (rc_dst < 0)
                                    goto e1;
                        dst->origin->object.close(&dst->origin->object);
                        // следующий origin
                        rc_src = src->origin->object.next(&src->origin->object);
                        if (rc_src < 0)
                                    goto e;
            }
            src->origin->object.close(&src->origin->object);

            return 0;

e1:
            dst->origin->object.close(&dst->origin->object);
e:
            src->origin->object.close(&src->origin->object);
            return -1;
}*/

Dbsync::Dbsync(Dbsync_origin *origin, Callback* callback)
    : origin(origin), statistics{db_sync_statistics()}, m_callback{callback}
{
    origin->sync_data = this;
}

int Dbsync::sync(Dbsync *other, char direction)
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

int Dbsync::sync_to(Dbsync *dst)
{
    int rc_src, rc_dst;

    // список origin в источнике
    rc_src = origin->open(nullptr);
    if (rc_src < 0)
        return -1;
    rc_src = origin->next();
    if (rc_src < 0)
        goto e;
    // идем по всем origin источника
    while (rc_src == DB_SYNC_OK) {
        LOG(LOG_INFO, "source origin %s, sync %d \n", origin->origin_string, origin->sync_number);
        // origin в приемнике
        rc_dst = dst->origin->open(origin->origin_string);
        if (rc_dst < 0) {
            printf("Cant open");
            goto e;
        }
        rc_dst = dst->origin->next();
        if (rc_dst < 0) {
            printf("Cant get next");
            goto e1;
        }
        if (rc_dst == DB_SYNC_EOF) {
            printf("No origin, trying to create \n");
            // если такого origin нет, создаем
            dst->origin->origin_string = strdup(origin->origin_string);
            rc_dst = dst->origin->add();
            if (rc_dst < 0)
                goto e1;
            LOG(LOG_INFO, "created new origin %s \n", dst->origin->origin_string);
            // переоткрываем, чтобы зацепить ID
            dst->origin->close();
            continue;
        }
        LOG(LOG_INFO, "destination origin %s, sync %d", dst->origin->origin_string, dst->origin->sync_number);
        // перемещаем данные
        rc_dst = sync_objects_to(dst);
        if (rc_dst < 0)
            goto e1;
        dst->origin->close();
        // следующий origin
        rc_src = origin->next();
        if (rc_src < 0)
            goto e;
    }
    origin->close();

    return 0;

e1:
    dst->origin->close();
e:
    origin->close();
    return -1;
}

/*int db_sync_copy_origins(db_sync_t *src, db_sync_t *dst)
{
            int rc_src, rc_dst;

            // список origin в источнике
            rc_src = src->origin->open(src->origin, NULL);
            if (rc_src < 0)
                        return -1;
            rc_src = src->origin->object.next(&src->origin->object);
            if (rc_src < 0)
                        goto e;
            // идем по всем origin источника
            while (rc_src == DB_SYNC_OK) {
                        // origin в приемнике
                        db_sync_origin_clear_data(&dst->origin->object);
                        dst->origin->origin_string = strdup(src->origin->origin_string);
                        dst->origin->object.sync_number = src->origin->object.sync_number;
                        rc_dst = dst->origin->object.add(&dst->origin->object);
                        if (rc_dst < 0)
                                    goto e;
                        // следующий origin
                        rc_src = src->origin->object.next(&src->origin->object);
                        if (rc_src < 0)
                                    goto e;
            }
            src->origin->object.close(&src->origin->object);
            return 0;
e:
            src->origin->object.close(&src->origin->object);
            return -1;
}*/


int Dbsync::copy_origins(Dbsync *dst)
{
    int rc_src, rc_dst;

    // список origin в источнике
    rc_src = this->origin->open(NULL);
    if (rc_src < 0)
        return -1;
    rc_src = this->origin->next();
    if (rc_src < 0)
        goto e;

    // идем по всем origin источника
    while (rc_src == DB_SYNC_OK) {
        // origin в приемнике
        dst->origin->clear_data();
        dst->origin->origin_string = strdup(this->origin->origin_string);
        dst->origin->sync_number = this->origin->sync_number;
        rc_dst = dst->origin->add();
        if (rc_dst < 0)
            goto e;
        // следующий origin
        rc_src = this->origin->next();
        if (rc_src < 0)
            goto e;
    }
    this->origin->close();
    return 0;

e:
    this->origin->close();
    return -1;
}

/*int _db_sync_origin(db_sync_t *src, db_sync_t *dst)
{
            int rc_src, rc_dst, written;
            db_sync_object_t **src_object, **dst_object;

            // на всякий случай проверяем одинаковость origin
            assert(strcmp(src->origin->origin_string,
                        dst->origin->origin_string) == 0);

            src_object = src->objects;
            while (*src_object) {
                        dst_object = dst->objects;
                        // ищем в приемнике объект такого типа
                        while (*dst_object) {
                                    if ((*src_object)->type == (*dst_object)->type) {
                                                // синхронизируем
                                                zlog_log("db_sync", ZLOG_INFO, "writing objects of type %d", (*dst_object)->type);
                                                written = 0;
                                                // открываем в источнике последние записи
                                                rc_src = (*src_object)->open(*src_object, src->origin->object.origin,
                                                            dst->origin->object.sync_number);
                                                if (rc_src < 0)
                                                            zerror_set(ERROR_ERROR);
                                                rc_src = (*src_object)->next(*src_object);
                                                if (rc_src < 0) {
                                                            zerror_set(ERROR_ERROR);
                                                            goto e;
                                                }
                                                // send steps from time to time
                                                if(_db_step(src, dst) < 0){
                                                            zerror_set(-1);
                                                            goto e;
                                                }
                                                while (rc_src == DB_SYNC_OK) {
                                                            (*src_object)->sync_data->statistics.objects_readed[(*src_object)->type]++;
                                                            // пишем в приемник
                                                            (*dst_object)->copy_data(*dst_object, *src_object);
                                                            db_sync_object_copy_data(*dst_object, *src_object);
                                                            // если объект из другой БД, номер origin у него другой
                                                            // надо исправить на номер из нашей базы
                                                            (*dst_object)->origin = (*dst_object)->sync_data->origin->object.origin;
                                                            rc_dst = (*dst_object)->add(*dst_object);
                                                            if (rc_dst < 0) {
                                                                        zerror_set(ERROR_ERROR);
                                                                        goto e;
                                                            }
                                                            (*dst_object)->sync_data->statistics.objects_added[(*dst_object)->type]++;
                                                            written++;
                                                            rc_src = (*src_object)->next(*src_object);
                                                            if (rc_src < 0) {
                                                                        zerror_set(ERROR_ERROR);
                                                                        goto e;
                                                            }
                                                            // send steps from time to time
                                                            if(_db_step(src, dst) < 0) {
                                                                        zerror_set(ERROR_ERROR);
                                                                        goto e;
                                                            }
                                                }
                                                (*src_object)->close(*src_object);
                                                zlog_log("db_sync", ZLOG_INFO, "written %d objects", written);
                                                // следующий в источнике
                                                break;
                                    }
                                    dst_object++;
                        }
                        src_object++;
            }
            // обновляем номер синхронизацыи
            if (src->origin->object.sync_number > dst->origin->object.sync_number) {
                        dst->origin->update(dst->origin, src->origin->object.sync_number);
                        zlog_log("db_sync", ZLOG_INFO, "set dst sync number to %d", src->origin->object.sync_number);
            }

            return 0;
e:
            zlog_log("db_sync", ZLOG_INFO, "written %d objects", written);
            (*src_object)->close(*src_object);
            return -1;
}*/

int Dbsync::sync_objects_to(Dbsync *dst)
{
    int rc_src, rc_dst, written;
    std::unique_ptr<Dbsync_object> *src_object, *dst_object;

    // на всякий случай проверяем одинаковость origin
    assert(strcmp(origin->origin_string, dst->origin->origin_string) == 0);

    // сброс статистики от прошлого origin
    origin->statistics.clear();
    dst->origin->statistics.clear();

    // последний делаем нулем для совместимости
    if(objects.back() != nullptr)
        objects.push_back(nullptr);
    if(dst->objects.back() != nullptr)
        dst->objects.push_back(nullptr);

    src_object = objects.data();
    if(!src_object)
        return -1;
    while (*src_object) {
        dst_object = dst->objects.data();
        // ищем в приемнике объект такого типа
        while (*dst_object) {
            if ((*src_object)->type == (*dst_object)->type) {
                // синхронизируем
                LOG(LOG_INFO, "writing objects of type %d", (*dst_object)->type);
                written = 0;
                // открываем в источнике последние записи
                rc_src = (*src_object)->open(origin->origin,
                                             dst->origin->sync_number);
                if (rc_src < 0)
                    ERROR(ERROR_ERROR);
                rc_src = (*src_object)->next();
                if (rc_src < 0) {
                    ERROR(ERROR_ERROR);
                    goto e;
                }
                // send steps from time to time
                if(do_step(dst) < 0){
                    ERROR(ERROR_ERROR);
                    goto e;
                }
                while (rc_src == DB_SYNC_OK) {
                    (*src_object)->sync_data->statistics.objects_readed[(*src_object)->type]++;
                    (*src_object)->sync_data->origin->statistics.objects_readed[(*src_object)->type]++;
                    // пишем в приемник
                    (*dst_object)->copy_data( *src_object );

                    // если объект из другой БД, номер origin у него другой
                    // надо исправить на номер из нашей базы
                    (*dst_object)->origin = (*dst_object)->sync_data->origin->origin;
                    rc_dst = (*dst_object)->add();
                    if (rc_dst < 0) {
                        ERROR(ERROR_ERROR);
                        goto e;
                    }
                    (*dst_object)->sync_data->statistics.objects_added[(*dst_object)->type]++;
                    (*dst_object)->sync_data->origin->statistics.objects_added[(*dst_object)->type]++;
                    written++;
                    rc_src = (*src_object)->next();
                    if (rc_src < 0) {
                        ERROR(ERROR_ERROR);
                        goto e;
                    }
                    // send steps from time to time
                    if(do_step(dst) < 0) {
                        ERROR(ERROR_ERROR);
                        goto e;
                    }
                }
                (*src_object)->close();
                LOG(LOG_INFO, "written %d objects", written);
                // следующий в источнике
                break;
            }
            dst_object++;
        }
        src_object++;
    }
    // обновляем номер синхронизацыи
    if (origin->sync_number > dst->origin->sync_number) {
        dst->origin->update(origin->sync_number);
        LOG(LOG_INFO, "set dst sync number to %d", origin->sync_number);
    }

    return 0;
e:
    LOG(LOG_INFO, "written %d objects", written);
    (*src_object)->close();
    return -1;
}

/*int _db_step(db_sync_t *src, db_sync_t *dst)
{
            static time_t timeout = 0;
            time_t now;

            now = time(NULL);
            if(now < timeout)
                        return 0;

            timeout = now + 2;

            zlog_log("db_sync", ZLOG_INFO, "step");

            if(src->step) {
                        if(src->step() < 0) {
                                    //zerror_message("sync_origin", -1, "Source step error");
                                    return zerror_set(ERROR_ERROR);
                        };
            }
            if(dst->step) {
                        if(dst->step() < 0) {
                                    //zerror_message("sync_origin", -2, "Destination step error");
                                    return zerror_set(ERROR_ERROR);
                        };
            }

            return 0;
}*/

int Dbsync::do_step(Dbsync *)
{
    static time_t timeout = 0;
    time_t now;

    now = time(NULL);
    if(now < timeout)
        return 0;

    timeout = now + 2;

    LOG(LOG_INFO, "step");

    m_callback->step();

//    if(step() < 0)
//        return ERROR(ERROR_ERROR);

//    if(dst->step() < 0)
//        return ERROR(ERROR_ERROR); // XXXX Не понятно почему убрали do step для dst надо разобраться

    return 0;
}

/*void db_sync_object_copy_data(db_sync_object_t *dst_object, db_sync_object_t *src_object)
{
            db_sync_object_clear_data(dst_object);

            dst_object->origin = src_object->origin;
            dst_object->sync_number = src_object->sync_number;
            dst_object->record_timestamp = strdup(src_object->record_timestamp);
}*/

void Dbsync_object::copy_data(std::unique_ptr<Dbsync_object> &from)
{
    origin = from->origin;
    sync_number = from->sync_number;
    delete record_timestamp;
    record_timestamp = strdup(from->record_timestamp);
}

/*void db_sync_uninit(db_sync_t *sync)
{
            db_sync_object_t **object;

            object = sync->objects;
            while (*object) {
                        (*object)->clear_data(*object);
                        db_sync_object_clear_data(*object);
                        free((*object)->container);
                        object++;
            }
            free(sync->objects);

            sync->origin->object.clear_data(&sync->origin->object);
            db_sync_object_clear_data(&sync->origin->object);
            free(sync->origin);
}

Dbsync::~Dbsync()
{
    Dbsync_object **object;

    object = objects;
    while (*object) {
        (*object)->clear_data();
        delete (*object)->container;
        object++;
    }
    delete objects;

    origin->clear_data();
    delete origin;
}

void db_sync_implantation_copy_data(db_sync_object_t *dst_object, db_sync_object_t *src_object)
{
            db_sync_implantation_t *src_implantation = (db_sync_implantation_t*) src_object->container;
            db_sync_implantation_t *dst_implantation = (db_sync_implantation_t*) dst_object->container;

            db_sync_implantation_clear_data(dst_object);

            dst_implantation->id = src_implantation->id;
            dst_implantation->pacemaker_model = src_implantation->pacemaker_model;
            dst_implantation->pacemaker_serial = src_implantation->pacemaker_serial;
            dst_implantation->patient_id = src_implantation->patient_id;
            dst_implantation->unique_patient_id = strdup(src_implantation->unique_patient_id);
}*/

void Dbsync_implantation::copy_data(std::unique_ptr<Dbsync_object> &src_object)
{
    Dbsync_implantation * src_implantation = dynamic_cast<Dbsync_implantation*>(src_object.get());

    clear_data();

    Dbsync_object::copy_data(src_object);

    id = src_implantation->id;
    pacemaker_model = src_implantation->pacemaker_model;
    pacemaker_serial = src_implantation->pacemaker_serial;
    patient_id = src_implantation->patient_id;
    delete unique_patient_id;
    unique_patient_id = strdup(src_implantation->unique_patient_id);
}

/*void db_sync_implantation_clear_data(db_sync_object_t *object)
{
            db_sync_implantation_t *implantation = (db_sync_implantation_t*) object->container;

            implantation->id = 0;
            implantation->pacemaker_model = 0;
            implantation->pacemaker_serial = 0;
            implantation->patient_id = 0;
            free(implantation->unique_patient_id);
            implantation->unique_patient_id = NULL;
}*/

void Dbsync_implantation::clear_data()
{
    Dbsync_object::clear_data();

    id = 0;
    pacemaker_model = 0;
    pacemaker_serial = 0;
    patient_id = 0;
    delete unique_patient_id;
    unique_patient_id = NULL;
}

/*void db_sync_patient_clear_data(db_sync_object_t *object)
{
            db_sync_patient_t *patient = (db_sync_patient_t*) object->container;

            free(patient->address);
            patient->address = NULL;
            memset(&patient->birth_date, 0, sizeof(patient->birth_date));
            free(patient->first_name);
            patient->first_name = NULL;
            free(patient->last_name);
            patient->last_name = NULL;
            free(patient->patronym);
            patient->patronym = NULL;
            free(patient->unique_id);
            patient->unique_id = NULL;
            patient->sex = 0;
}*/

void Dbsync_patient::clear_data()
{
    Dbsync_object::clear_data();

    delete this->address;
    this->address = nullptr;
    memset(&this->birth_date, 0, sizeof(this->birth_date));
    delete first_name;
    first_name = nullptr;
    delete last_name;
    last_name = nullptr;
    delete patronym;
    patronym = nullptr;
    delete unique_id;
    unique_id = nullptr;
    sex = 0;
}

/*void db_sync_patient_copy_data(db_sync_object_t *dst_object, db_sync_object_t *src_object)
{
            db_sync_patient_t *src_patient = (db_sync_patient_t*) src_object->container;
            db_sync_patient_t *dst_patient = (db_sync_patient_t*) dst_object->container;

            db_sync_patient_clear_data(dst_object);

            dst_patient->address = strdup(src_patient->address);
            dst_patient->birth_date = src_patient->birth_date;
            dst_patient->first_name = strdup(src_patient->first_name);
            dst_patient->last_name = strdup(src_patient->last_name);
            dst_patient->patronym = strdup(src_patient->patronym);
            if (src_patient->unique_id)
                        dst_patient->unique_id = strdup(src_patient->unique_id);
            dst_patient->sex = src_patient->sex;
}*/

void Dbsync_patient::copy_data(std::unique_ptr<Dbsync_object> &src_object)
{
    Dbsync_patient *src_patient = dynamic_cast<Dbsync_patient*>(src_object.get());

    clear_data();

    Dbsync_object::copy_data(src_object);

    address = strdup(src_patient->address);
    birth_date = src_patient->birth_date;
    first_name = strdup(src_patient->first_name);
    last_name = strdup(src_patient->last_name);
    patronym = strdup(src_patient->patronym);
    if (src_patient->unique_id)
        unique_id = strdup(src_patient->unique_id);
    sex = src_patient->sex;
}


/*void db_sync_session_clear_data(db_sync_object_t *object)
{
            db_sync_session_t *session = (db_sync_session_t*) object->container;

            session->id = 0;
            memset(&session->date, 0, sizeof(session->date));
            session->model = 0;
            session->serial = 0;
            session->type = 0;
            session->length = 0;
            free(session->data);
            session->data = NULL;
}*/

void Dbsync_session::clear_data()
{
//    db_sync_session_t *session = (db_sync_session_t*) object->container;
    Dbsync_object::clear_data();

    id = 0;
    memset(&date, 0, sizeof(date));
    model = 0;
    serial = 0;
    session_type = 0;
    length = 0;
    delete session_data;
    session_data = nullptr;
}

/*void db_sync_session_copy_data(db_sync_object_t *dst_object, db_sync_object_t *src_object)
{
            db_sync_session_t *src_session = (db_sync_session_t*) src_object->container;
            db_sync_session_t *dst_session = (db_sync_session_t*) dst_object->container;

            db_sync_session_clear_data(dst_object);

            dst_session->id = src_session->id;
            dst_session->date = src_session->date;
            dst_session->model = src_session->model;
            dst_session->serial = src_session->serial;
            dst_session->type = src_session->type;
            dst_session->length = src_session->length;
            dst_session->data = malloc(dst_session->length);
            memcpy(dst_session->data, src_session->data, dst_session->length);
}*/

void Dbsync_session::copy_data(std::unique_ptr<Dbsync_object> &src_object)
{
    Dbsync_session *src_session = dynamic_cast<Dbsync_session*>(src_object.get());

    clear_data();

    Dbsync_object::copy_data(src_object);

    id = src_session->id;
    date = src_session->date;
    model = src_session->model;
    serial = src_session->serial;
    session_type = src_session->session_type;
    length = src_session->length;
    session_data = new unsigned char [length];
    memcpy(session_data, src_session->session_data, length);
}
