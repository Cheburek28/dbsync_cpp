#include <stdio.h>

#include "log/error.h"
#include "sqlite3.h"
#include "dbsync_sqlite.h"

#include <assert.h>

// disable strdup warning
//#pragma warning( disable : 4996 )

int Dbsync_sqlite::m_error_code = -1;

Dbsync_sqlite::Dbsync_sqlite(const char *filename, Callback* callback)
    : Dbsync(new Dbsync_origin_sqlite, callback), m_filename(filename)
{
    objects.emplace(objects.begin(), new Dbsync_session_sqlite());
    objects.front()->sync_data = this;
    objects.emplace(objects.begin(), new Dbsync_implantation_sqlite());
    objects.front()->sync_data = this;
    objects.emplace(objects.begin(), new Dbsync_patient_sqlite());
    objects.front()->sync_data = this;

    m_error_code = error()->add_handler("ERROR_DBSYNC_SQLITE", Dbsync_sqlite::error_handler);
}

int Dbsync_sqlite::open_source()
{
    sqlite3 *dbsync_handle, *dbfile_handle = NULL;
    sqlite3_backup *backup_handle;

    int x;
    //создаем бд в памяти
    x = sqlite3_open(":memory:", &dbsync_handle);
    if (x)
        return -1;
    // ищем файл
    x =sqlite3_open_v2(m_filename.c_str(), &dbfile_handle, SQLITE_OPEN_READWRITE, NULL);
    if (x) {
        //пробуем создать
        x = sqlite3_open(m_filename.c_str(), &dbfile_handle);
        if (x) {
            sqlite3_close(dbfile_handle);
            return -1;
        }
    } else {
        // если есть, копируем с диска в память
        backup_handle = sqlite3_backup_init(dbsync_handle, "main", dbfile_handle, "main");
        if (backup_handle == NULL) {
            sqlite3_close(dbfile_handle);
            return -1;
        }
        if (sqlite3_backup_step(backup_handle, -1) < 0) {
            sqlite3_backup_finish(backup_handle);
            sqlite3_close(dbfile_handle);
            return -1;
        }
        int err = sqlite3_backup_finish(backup_handle);
        if (err) {
            sqlite3_close(dbfile_handle);
            return -1;
        }
    }
    // инициализируем базу всегда на случай, если открыли пустой файл
    create_tables(dbsync_handle);
    sqlite3_close(dbfile_handle);

    // проверяем наличие лога синхронизации
    data_source = dbsync_handle;

    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(dbsync_handle,
                       "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='tbl_sync_register'",
                       -1, &stmt, NULL);
    sqlite3_step(stmt);
    if(sqlite3_column_int(stmt, 0) == 1)
        sync_log = true;
    sqlite3_finalize(stmt);

    return 0;
}

int Dbsync_sqlite::close_source()
{
    sqlite3 *dbfile_handle;
    sqlite3 *dbsync_handle = (sqlite3*)data_source;
    sqlite3_backup *backup_handle;

    if(!data_source)
        return 0;

    if (sqlite3_open(m_filename.c_str(), &dbfile_handle))
        return -1;
    // пишем все в файл
    backup_handle = sqlite3_backup_init(dbfile_handle, "main", dbsync_handle, "main");
    if (backup_handle == NULL) {
        sqlite3_close(dbfile_handle);
        return -1;
    }
    if (sqlite3_backup_step(backup_handle, -1) < 0) {
        sqlite3_backup_finish(backup_handle);
        sqlite3_close(dbfile_handle);
        return -1;
    }
    if (sqlite3_backup_finish(backup_handle)) {
        sqlite3_close(dbfile_handle);
        return -1;
    }
    sqlite3_close(dbfile_handle);
    sqlite3_close(dbsync_handle);

    data_source = nullptr;

    return 0;
}



/*int _db_sync_sql_origin(char *sql, char *origin_id)
{
    if (origin_id && (*origin_id))
        return sprintf(sql, "select nID, strID, nLastSyncNumber from tbl_sync_origins where strID=\"%s\"", origin_id);
    else
        return sprintf(sql, "select nID, strID, nLastSyncNumber from tbl_sync_origins");
}

int _db_sync_sqlite_origin_open(db_sync_origin_t *origin, char *origin_id)
{
    char text[500];

    _db_sync_sql_origin(text, origin_id);
    return _db_sync_sqlite_prepare(text, &origin->object);
}*/

int Dbsync_origin_sqlite::open(const char *origin_id)
{
    char text[500];

    if (origin_id && (*origin_id))
        sprintf(text, "select nID, strID, nLastSyncNumber from tbl_sync_origins where strID=\"%s\"", origin_id);
    else
        sprintf(text, "select nID, strID, nLastSyncNumber from tbl_sync_origins");

    return Dbsync_sqlite::prepare(text, this);
}


/*int _db_sync_sqlite_origin_update(db_sync_origin_t *origin, int sync_number)
{
    char text[500];

    _db_sync_sql_increment(text, sync_number, origin->object.origin);
    // инкремент номера синхронизации
    if (sqlite3_exec((sqlite3*) origin->object.sync_data->data_source,
        text, NULL, NULL, NULL) != SQLITE_OK) {
        return zerror_set(ERROR_DB, origin->object.sync_data, text);
    }

    return 0;
}
int _db_sync_sql_increment(char *sql, int sync_number, int origin)
{
    return sprintf(sql, "update tbl_sync_origins set nLastSyncNumber=%d \
                   where nID=%d", sync_number, origin);
}



int _db_sync_sqlite_patient_open(db_sync_object_t *object, int origin, int sync_number)
{
    char text[1000];

    _db_sync_sql_patient(text, origin, sync_number);
    return _db_sync_sqlite_prepare(text, object);
}
int _db_sync_sql_patient(char *sql, int origin, int sync_number)
{
    return sprintf(sql, "select tbl_patients.nID, tbl_patients.strFirstName, \
        tbl_patients.strLastName, tbl_patients.strPatronym, tbl_patients.strAddress, \
        tbl_patients.dtBirthDate, tbl_patients.strUniqueID, tbl_patients.dtRecordTimestamp, \
        tbl_patients.nSyncNumber, tbl_patients.nOrigin, tbl_patients.nSex \
        from tbl_patients where tbl_patients.nSyncNumber>%d and tbl_patients.nOrigin=%d", sync_number, origin);
}*/
int Dbsync_patient_sqlite::open(int origin, int sync_number)
{
    char text[1000];

    sprintf(text, "select tbl_patients.nID, tbl_patients.strFirstName, \
            tbl_patients.strLastName, tbl_patients.strPatronym, tbl_patients.strAddress, \
            tbl_patients.dtBirthDate, tbl_patients.strUniqueID, tbl_patients.dtRecordTimestamp, \
            tbl_patients.nSyncNumber, tbl_patients.nOrigin, tbl_patients.nSex \
            from tbl_patients where tbl_patients.nSyncNumber>%d and tbl_patients.nOrigin=%d", sync_number, origin);

    return Dbsync_sqlite::prepare(text, this);
}

/*int _db_sync_sql_patient_update(char *sql, db_sync_patient_t *patient)
{
    char date[20];

    sprintf(date, "%04d-%02d-%02d", patient->birth_date.tm_year, patient->birth_date.tm_mon, patient->birth_date.tm_mday);
    return sprintf(sql, "update tbl_patients set strFirstName=\"%s\", \
                   strLastName=\"%s\", strPatronym=\"%s\", strAddress=\"%s\", \
                   dtBirthDate=\"%s\", dtRecordTimestamp=\"%s\", nSyncNumber=%d, \
                   nOrigin=%d, nSex=%d where strUniqueID=\"%s\"",
        patient->first_name, patient->last_name, patient->patronym,
        patient->address, date, patient->object.record_timestamp,
        patient->object.sync_number, patient->object.origin,
        patient->sex, patient->unique_id);
}*/
int Dbsync_patient_sqlite::update()
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
    if (sqlite3_exec((sqlite3*)  sync_data->data_source,
        sql, NULL, NULL, NULL) != SQLITE_OK) {
        return ERROR_DATA(Dbsync_sqlite::error_code(), sync_data, sql);
    }

    return 0;
}


/*int _db_sync_sqlite_patient_close(db_sync_object_t *object)
{
    sqlite3_stmt *stmt = (sqlite3_stmt*) object->data;

    assert(object->type == DB_SYNC_OBJECT_PATIENT);

    sqlite3_finalize(stmt);
    db_sync_patient_clear_data(object->container);
    db_sync_object_clear_data(object);

    return 0;
}*/
int Dbsync_patient_sqlite::close()
{
    sqlite3_stmt *stmt = (sqlite3_stmt*) data;

    assert(type == DB_SYNC_OBJECT_PATIENT);

    sqlite3_finalize(stmt);
    clear_data();

    return 0;
}

/*int _db_sync_sqlite_patient_next(db_sync_object_t *object)
{
    int rc;
    sqlite3_stmt *stmt = (sqlite3_stmt*) object->data;
    db_sync_patient_t *patient = (db_sync_patient_t*) object->container;
    char *unique_id;

    rc = _db_sync_sqlite_step(object);
    if (rc < 0)
        return rc;

    db_sync_patient_clear_data(object);
    db_sync_object_clear_data(object);
    if (rc == DB_SYNC_EOF)
        return DB_SYNC_EOF;

    patient->id = sqlite3_column_int(stmt, 0);
    patient->first_name = strdup((char*) sqlite3_column_text(stmt, 1));
    patient->last_name = strdup((char*) sqlite3_column_text(stmt, 2));
    patient->patronym = strdup((char*) sqlite3_column_text(stmt, 3));
    patient->address = strdup((char*) sqlite3_column_text(stmt, 4));
    sscanf((char*) sqlite3_column_text(stmt, 5), "%d-%d-%d", &patient->birth_date.tm_year, &patient->birth_date.tm_mon,
        &patient->birth_date.tm_mday);
    unique_id = (char*) sqlite3_column_text(stmt, 6);
    if (unique_id != NULL)
        patient->unique_id = strdup(unique_id);

    patient->object.record_timestamp = strdup((char*) sqlite3_column_text(stmt, 7));
    patient->object.sync_number = sqlite3_column_int(stmt, 8);
    patient->object.origin = sqlite3_column_int(stmt, 9);
    patient->sex = sqlite3_column_int(stmt, 10);

    return DB_SYNC_OK;
}*/
int Dbsync_patient_sqlite::next()
{
    int rc;
    sqlite3_stmt *stmt = (sqlite3_stmt*) data;
    char *unique_id;

    rc = Dbsync_sqlite::step(this);
    if (rc < 0)
        return rc;

    clear_data();
    if (rc == DB_SYNC_EOF)
        return DB_SYNC_EOF;

    id = sqlite3_column_int(stmt, 0);
    first_name = strdup((char*) sqlite3_column_text(stmt, 1));
    last_name = strdup((char*) sqlite3_column_text(stmt, 2));
    patronym = strdup((char*) sqlite3_column_text(stmt, 3));
    address = strdup((char*) sqlite3_column_text(stmt, 4));
    sscanf((char*) sqlite3_column_text(stmt, 5), "%d-%d-%d", &birth_date.tm_year, &birth_date.tm_mon,
           &birth_date.tm_mday);
    unique_id = (char*) sqlite3_column_text(stmt, 6);
    if (unique_id != NULL)
        this->unique_id = strdup(unique_id);

    record_timestamp = strdup((char*) sqlite3_column_text(stmt, 7));
    sync_number = sqlite3_column_int(stmt, 8);
    origin = sqlite3_column_int(stmt, 9);
    sex = sqlite3_column_int(stmt, 10);

    return DB_SYNC_OK;
}


/*int _db_sync_sqlite_patient_add(db_sync_object_t *object)
{
    char text[1000];
    int rc;
    db_sync_patient_t *patient = (db_sync_patient_t *) object->container;

    // сперва пробуем обновить
    sprintf(text, "select nID from tbl_patients where \
            strUniqueID=\"%s\"", patient->unique_id);
    _db_sync_sqlite_prepare(text, object);
    rc = _db_sync_sqlite_step(object);
    sqlite3_finalize((sqlite3_stmt*) object->data);
    if (rc < 0)
        return rc;
    if (rc == DB_SYNC_OK) {
        _db_sync_sql_patient_update(text, patient);
        rc = sqlite3_exec((sqlite3*) object->sync_data->data_source, text,
            NULL, NULL, NULL);
        if (rc)
            return zerror_set(ERROR_DB, object->sync_data, text);
    } else {
        _db_sync_sql_patient_insert(text, patient);
        rc = sqlite3_exec((sqlite3*) object->sync_data->data_source, text, NULL,
            NULL, NULL);
        if (rc)
            return zerror_set(ERROR_DB, object->sync_data, text);
    }
    return 0;
}
int _db_sync_sql_patient_insert(char *sql, db_sync_patient_t *patient)
{
    char date[20];

    sprintf(date, "%d-%d-%d", patient->birth_date.tm_year, patient->birth_date.tm_mon, patient->birth_date.tm_mday);
    return sprintf(sql, "insert into tbl_patients (strFirstName, \
                   strLastName, strPatronym, strAddress, dtBirthDate, \
                   dtRecordTimestamp, nSyncNumber, nOrigin, strUniqueID, nSex) \
                   values ('%s', '%s', '%s', '%s', '%s', '%s', %d, %d, '%s', %d)",
        patient->first_name, patient->last_name, patient->patronym,
        patient->address, date, patient->object.record_timestamp,
        patient->object.sync_number, patient->object.origin,
        patient->unique_id, patient->sex);
}*/

int Dbsync_patient_sqlite::add()
{
    char text[1000];
    int rc;

    // сперва пробуем обновить
    sprintf(text, "select nID from tbl_patients where \
            strUniqueID=\"%s\"", unique_id);
    Dbsync_sqlite::prepare(text, this);
    rc = Dbsync_sqlite::step(this);
    sqlite3_finalize((sqlite3_stmt*) data);
    if (rc < 0)
        return rc;
    if (rc == DB_SYNC_OK) {
        update();
        rc = sqlite3_exec((sqlite3*) sync_data->data_source, text,
                          NULL, NULL, NULL);
        if (rc)
            return ERROR_DATA(Dbsync_sqlite::error_code(), sync_data, text);
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
        rc = sqlite3_exec((sqlite3*) sync_data->data_source, text, NULL,
                          NULL, NULL);
        if (rc)
            return ERROR_DATA(Dbsync_sqlite::error_code(), sync_data, text);
    }
    return 0;
}


/*int _db_sync_sqlite_prepare(char *sql, db_sync_object_t *object)
{
    sqlite3_stmt *stmt;
    sqlite3 *ds;
    int rc;

    ds = (sqlite3*)object->sync_data->data_source;
    printf("%s\n", sql);
    rc = sqlite3_prepare_v2(ds, sql, -1, &stmt, NULL);
    if (rc)
        return zerror_set(ERROR_DB, object->sync_data, sql);
    object->data = stmt;

    return -rc;
}*/


int Dbsync_origin_sqlite::update(int sync_number)
{
    char text[500];

    sprintf(text, "update tbl_sync_origins set nLastSyncNumber=%d where nID=%d",
            sync_number, origin);
    // инкремент номера синхронизации
    if (sqlite3_exec((sqlite3*)  sync_data->data_source, text, NULL, NULL, NULL) != SQLITE_OK)
        return ERROR_DATA(Dbsync_sqlite::error_code(), sync_data, text);

    // запись статистики в лог
    if(static_cast<Dbsync_sqlite*>(sync_data)->sync_log) {
        sprintf(text, "INSERT INTO tbl_sync_log (origin, origin_name, patient, implantation, session)"
            "VALUES (%d, \"%s\", %d, %d, %d)", origin, origin_string,
                statistics.objects_added[DB_SYNC_OBJECT_PATIENT],
                statistics.objects_added[DB_SYNC_OBJECT_IMPLANTATION],
                statistics.objects_added[DB_SYNC_OBJECT_SESSION]);
        if (sqlite3_exec((sqlite3*)  sync_data->data_source, text, NULL, NULL, NULL) != SQLITE_OK)
            return ERROR_DATA(Dbsync_sqlite::error_code(), sync_data, text);
    }

    return 0;
}

/*int _db_sync_sqlite_prepare(char *sql, db_sync_object_t *object)
{
    sqlite3_stmt *stmt;
    sqlite3 *ds;
    int rc;

    ds = (sqlite3*)object->sync_data->data_source;
    printf("%s\n", sql);
    rc = sqlite3_prepare_v2(ds, sql, -1, &stmt, NULL);
    if (rc)
        return zerror_set(ERROR_DB, object->sync_data, sql);
    object->data = stmt;

    return -rc;
}

int _db_sync_sqlite_error_sql(char *ubuf, int ulen, char *buf, int len, int *module_code, va_list ap)
{
    db_sync_t *sync = va_arg(ap, db_sync_t*);
    const char* sql = va_arg(ap, const char*);

    *module_code = sqlite3_errcode((sqlite3*) sync->data_source);

    return snprintf(buf, len, "%s (%d), sql: %s",
        sqlite3_errmsg((sqlite3*) sync->data_source), *module_code, sql);

}*/

void Dbsync_sqlite::error_handler(char *user_buf, int user_len, char *system_buf, int system_len, int, va_list ap)
{
    Dbsync *sync = va_arg(ap, Dbsync*);
    const char * sql = va_arg(ap, const char*);

    // текст пользователю
    snprintf(user_buf, user_len, "Database synchronisation error. Please contact the developer.");

    snprintf(system_buf, system_len, "%s (%d), sql: %s",
             sqlite3_errmsg((sqlite3*) sync->data_source), sqlite3_errcode((sqlite3*) sync->data_source), sql);
}

/*int _db_sync_sqlite_origin_close(db_sync_object_t *object)
{
    sqlite3_stmt *stmt = (sqlite3_stmt*) object->data;
    sqlite3_finalize(stmt);
    db_sync_origin_clear_data(object);
    db_sync_object_clear_data(object);

    return 0;
}*/

int Dbsync_origin_sqlite::close()
{
    sqlite3_stmt *stmt = (sqlite3_stmt*) this->data;
    sqlite3_finalize(stmt);

    Dbsync_origin::close();

    return 0;
}


/*int _db_sync_sqlite_origin_next(db_sync_object_t *object)
{
    int rc;
    sqlite3_stmt *stmt = (sqlite3_stmt*) object->data;
    db_sync_origin_t *origin = (db_sync_origin_t*) object->container;

    rc = _db_sync_sqlite_step(object);
    if (rc < 0)
        return rc;

    db_sync_origin_clear_data(&origin->object);
    db_sync_object_clear_data(&origin->object);
    if (rc == DB_SYNC_EOF)
        return DB_SYNC_EOF;

    origin->object.origin = sqlite3_column_int(stmt, 0);
    origin->origin_string = strdup((char*) sqlite3_column_text(stmt, 1));
    origin->object.sync_number = sqlite3_column_int(stmt, 2);

    return DB_SYNC_OK;
}*/

int Dbsync_origin_sqlite::next()
{
    int rc;
    sqlite3_stmt *stmt = (sqlite3_stmt*) data;

    rc = Dbsync_sqlite::step(this);
    if (rc < 0)
        return rc;

    clear_data();

    if (rc == DB_SYNC_EOF)
        return DB_SYNC_EOF;

    origin = sqlite3_column_int(stmt, 0);
    origin_string = strdup((char*) sqlite3_column_text(stmt, 1));
    sync_number = sqlite3_column_int(stmt, 2);

    return DB_SYNC_OK;
}

/*int _db_sync_sql_origin_add(char *sql, char *origin, int sync_number)
{
    return sprintf(sql, "INSERT INTO tbl_sync_origins (strID, nLastSyncNumber) VALUES (\"%s\", %d)",
        origin, sync_number);
}

int _db_sync_sqlite_origin_add(db_sync_object_t *object)
{
    char text[500];
    int rc;
    db_sync_origin_t *origin = (db_sync_origin_t*) object->container;

    _db_sync_sql_origin_add(text, origin->origin_string, origin->object.sync_number);
    rc = sqlite3_exec((sqlite3*) object->sync_data->data_source, text, NULL, NULL, NULL);
    if (rc)
        return zerror_set(ERROR_DB, object->sync_data, text);
    return 0;
}*/

int Dbsync_origin_sqlite::add()
{
    char text[500];
    int rc;

    sprintf(text, "INSERT INTO tbl_sync_origins (strID, nLastSyncNumber) VALUES (\"%s\", %d)",
            origin_string, sync_number);

    rc = sqlite3_exec((sqlite3*) sync_data->data_source, text, NULL, NULL, NULL);
    if (rc)
        return ERROR_DATA(Dbsync_sqlite::error_code(), sync_data, text);
    return 0;
}

/*int _db_sync_sqlite_step(db_sync_object_t *object)
{
    int rc;
    sqlite3_stmt *stmt = (sqlite3_stmt*) object->data;

    rc = sqlite3_step(stmt);
    switch (rc) {
    case SQLITE_ROW:
        return DB_SYNC_OK;
    case SQLITE_DONE:
        return DB_SYNC_EOF;
    default:
        return zerror_set(ERROR_DB, object->sync_data, sqlite3_sql(stmt));
    }
}*/

int Dbsync_sqlite::step(Dbsync_object *object)
{
    int rc;
    sqlite3_stmt *stmt = (sqlite3_stmt*) object->data;

    rc = sqlite3_step(stmt);
    switch (rc) {
    case SQLITE_ROW:
        return DB_SYNC_OK;
    case SQLITE_DONE:
        return DB_SYNC_EOF;
    default:
        return ERROR_DATA(Dbsync_sqlite::error_code(), object->sync_data, sqlite3_sql(stmt));
    }
}

int Dbsync_sqlite::prepare(char *sql, Dbsync_object *object)
{
    sqlite3_stmt *stmt;
    sqlite3 *ds;
    int rc;

    ds = (sqlite3*)object->sync_data->data_source;
//    printf("%s\n", sql);
    rc = sqlite3_prepare_v2(ds, sql, -1, &stmt, NULL);
    if (rc)
        return ERROR_DATA(Dbsync_sqlite::error_code(), object->sync_data, sql);
    object->data = stmt;

    return -rc;
}

//// sync by temporary file
/*int db_sync_sqlite(db_sync_t *sync, const char *dbsyncpath, char direction)
{
    sqlite3 *dbsync, *dbfile = NULL;
    sqlite3_backup *backup;
    db_sync_t *sqlite_sync;
    char *dbsyncfile;

    //создаем бд в памяти
    if (sqlite3_open(":memory:", &dbsync))
        return -1;
    dbsyncfile = db_sync_sqlite_create_full_filename(dbsyncpath);
    // ищем файл
    if (sqlite3_open_v2(dbsyncfile, &dbfile, SQLITE_OPEN_READWRITE, NULL)) {
        //пробуем создать
        if (sqlite3_open(dbsyncfile, &dbfile))
            goto e2;
    } else {
        // если есть, копируем с диска в память
        backup = sqlite3_backup_init(dbsync, "main", dbfile, "main");
        if (backup == NULL)
            goto e1;
        if (sqlite3_backup_step(backup, -1) < 0) {
            sqlite3_backup_finish(backup);
            goto e1;
        }
        if (sqlite3_backup_finish(backup))
            goto e1;
    }
    // инициализируем базу всегда на случай, если открыли пустой файл
    db_sync_sqlite_init_db(dbsync);
    sqlite3_close(dbfile);

    db_sync_create(&sqlite_sync);
    db_sync_init_sqlite(sqlite_sync);
    sqlite_sync->data_source = dbsync;

    // в одну сторону
    if(direction & DB_SYNC_DIRECTION_TO) {
        zlog_log("db_sync", ZLOG_INFO, "syncing from local");
        if(sqlite_sync->trans_begin(sqlite_sync) < 0)
            goto e;
        if (_db_sync(sync, sqlite_sync) < 0) {
            sqlite_sync->trans_rollback(sqlite_sync);
            goto e;
        }
        if(sync->increment_sync(sync) < 0) {
            sqlite_sync->trans_rollback(sqlite_sync);
            goto e;
        }
        zlog_log("db_sync", ZLOG_INFO, "local sync incremented");
        if(sqlite_sync->trans_commit(sqlite_sync) < 0)
            goto e;
    }
    // теперь в другую
    if(direction & DB_SYNC_DIRECTION_FROM) {
        zlog_log("db_sync", ZLOG_INFO, "syncing to local");
        if(sync->trans_begin(sync) < 0)
            goto e;
        if (_db_sync(sqlite_sync, sync) < 0) {
            sync->trans_rollback(sync);
            goto e;
        }
        if(sync->trans_commit(sync) < 0)
            goto e;
    }

    if (sqlite3_open(dbsyncfile, &dbfile))
        goto e;
    // пишем все в файл
    backup = sqlite3_backup_init(dbfile, "main", dbsync, "main");
    if (backup == NULL)
        goto e;
    if (sqlite3_backup_step(backup, -1) < 0) {
        sqlite3_backup_finish(backup);
        goto e;
    }
    if (sqlite3_backup_finish(backup))
        goto e;

    free(dbsyncfile);
    sqlite3_close(dbfile);
    sqlite3_close(dbsync);
    db_sync_uninit(sqlite_sync);
    db_sync_destroy(sqlite_sync);
    return 0;

e:
    db_sync_uninit(sqlite_sync);
    db_sync_destroy(sqlite_sync);
e1:
    sqlite3_close(dbfile);
e2:
    free(dbsyncfile);
    sqlite3_close(dbsync);
    return -1;
}*/

int Dbsync_sqlite::sync(Dbsync *other, char direction)
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


/*char *db_sync_sqlite_create_full_filename(const char *dbsyncpath)
{
    char *dbsyncfile;

    dbsyncfile = malloc(1000);
    if (dbsyncpath)
        sprintf(dbsyncfile, "%s/%s", dbsyncpath, db_sync_sqlite_get_filename());
    else
        strcpy(dbsyncfile, db_sync_sqlite_get_filename());

    return dbsyncfile;
}

const char * db_sync_sqlite_get_filename()
{
    return "unisync.db";
}*/

char *Dbsync_sqlite::create_full_filename(const char *dbsyncpath)
{
    char *dbsyncfile;

    dbsyncfile = new char[1000];
    if (dbsyncpath)
        sprintf(dbsyncfile, "%s/%s", dbsyncpath, m_filename.c_str());
    else
        strcpy(dbsyncfile, m_filename.c_str());

    return dbsyncfile;
}


/*int db_sync_sqlite_init_db(sqlite3 *dbsync)
{
    // создаем бд с нуля
    sqlite3_exec(dbsync, "CREATE TABLE tbl_sync_origins (nID INTEGER PRIMARY KEY AUTOINCREMENT, strID text, \
    nLastSyncNumber integer)",
        NULL, NULL, NULL);
    sqlite3_exec(dbsync, "CREATE TABLE tbl_sessions (nID INTEGER PRIMARY KEY AUTOINCREMENT, nPacemakerModel \
        INTEGER, nPacemakerSerial INTEGER, nSessionType INTEGER, nSessionData BLOB, \
        dtSessionDate DATETIME, dtRecordTimestamp DATETIME, \
        nSyncNumber integer, nOrigin integer)", NULL, NULL, NULL);
    sqlite3_exec(dbsync, "CREATE TABLE tbl_patients (nID integer primary key autoincrement, strFirstName text, \
        strLastName text, strPatronym text, nSex integer, strAddress text, dtBirthDate date, dtRecordTimestamp DATETIME, \
        nSyncNumber integer, nOrigin integer, strUniqueID text)", NULL, NULL, NULL);
    sqlite3_exec(dbsync, "CREATE TABLE tbl_implantations (nID integer primary key autoincrement, \
        nPatientID integer, nPacemakerModel integer, nPacemakerSerial integer, dtRecordTimestamp DATETIME, \
        nSyncNumber integer, nOrigin integer)", NULL, NULL, NULL);
    return 0;
}*/

int Dbsync_sqlite::create_tables(sqlite3 *dbsync)
{
    // создаем бд с нуля
    sqlite3_exec(dbsync, "CREATE TABLE IF NOT EXISTS tbl_sync_origins (nID INTEGER PRIMARY KEY AUTOINCREMENT, strID text, \
    nLastSyncNumber integer)",
        NULL, NULL, NULL);
    sqlite3_exec(dbsync, "CREATE TABLE IF NOT EXISTS tbl_sessions (nID INTEGER PRIMARY KEY AUTOINCREMENT, nPacemakerModel \
        INTEGER, nPacemakerSerial INTEGER, nSessionType INTEGER, nSessionData BLOB, \
        dtSessionDate DATETIME, dtRecordTimestamp DATETIME, \
        nSyncNumber integer, nOrigin integer)", NULL, NULL, NULL);
    sqlite3_exec(dbsync, "CREATE TABLE IF NOT EXISTS tbl_patients (nID integer primary key autoincrement, strFirstName text, \
        strLastName text, strPatronym text, nSex integer, strAddress text, dtBirthDate date, dtRecordTimestamp DATETIME, \
        nSyncNumber integer, nOrigin integer, strUniqueID text)", NULL, NULL, NULL);
    sqlite3_exec(dbsync, "CREATE TABLE IF NOT EXISTS tbl_implantations (nID integer primary key autoincrement, \
        nPatientID integer, nPacemakerModel integer, nPacemakerSerial integer, dtRecordTimestamp DATETIME, \
        nSyncNumber integer, nOrigin integer)", NULL, NULL, NULL);
            return 0;
}

/*int _db_sync_sqlite_increment(db_sync_t *sync)
{
    int rc;
    char *sql;

    sql = "update tbl_sync_origins set nLastSyncNumber=((select nLastSyncNumber from \
        tbl_sync_origins where nID=1)+1) where nID=1";
    rc = sqlite3_exec((sqlite3*) sync->data_source, sql, NULL, NULL, NULL);
    if (rc)
        return zerror_set(ERROR_DB, sync, sql);

    return 0;
}*/

int Dbsync_sqlite::increment_sync()
{
    int rc;
    const char *sql;

    sql = "update tbl_sync_origins set nLastSyncNumber=((select nLastSyncNumber from \
            tbl_sync_origins where nID=1)+1) where nID=1";
    rc = sqlite3_exec((sqlite3*) data_source, sql, NULL, NULL, NULL);
    if (rc)
        return ERROR_DATA(Dbsync_sqlite::error_code(), this, sql);

    return 0;
}

/*int _db_sync_sqlite_exec(db_sync_t *sync, char *sql)
{
    int rc;

    rc = sqlite3_exec((sqlite3*)sync->data_source, sql, NULL, NULL, NULL);
    if (rc)
        return zerror_set(ERROR_DB, sync, sql);
    return 0;
}*/

int Dbsync_sqlite::exec(const char *sql)
{
    int rc;

    rc = sqlite3_exec((sqlite3*)data_source, sql, NULL, NULL, NULL);
    if (rc)
        return ERROR_DATA(Dbsync_sqlite::error_code(), this, sql);
    return 0;
}

/*int _db_sync_trans_begin(db_sync_t *sync)
{
    //стартуем транзакцию
    return _db_sync_sqlite_exec(sync, "BEGIN");
}*/

int Dbsync_sqlite::trans_begin()
{
    //стартуем транзакцию
    return exec("BEGIN");
}

/*int _db_sync_trans_commit(db_sync_t *sync)
{
    return _db_sync_sqlite_exec(sync, "COMMIT");
}*/

int Dbsync_sqlite::trans_commit()
{
    return exec("COMMIT");
}

/*int _db_sync_trans_rollback(db_sync_t *sync)
{
    return _db_sync_sqlite_exec(sync, "ROLLBACK");
}*/

int Dbsync_sqlite::trans_rollback()
{
    return exec("ROLLBACK");
}

/*int _db_sync_sql_implantation(char *sql, int origin, int sync_number)
{
    return sprintf(sql, "select tbl_implantations.nID, tbl_implantations.nPatientID, \
        tbl_patients.strUniqueID, \
        tbl_implantations.nPacemakerModel, tbl_implantations.nPacemakerSerial, \
        tbl_implantations.dtRecordTimestamp, tbl_implantations.nSyncNumber, \
        tbl_implantations.nOrigin from tbl_implantations \
        left join tbl_patients on tbl_implantations.nPatientID=tbl_patients.nID \
        where tbl_implantations.nSyncNumber>%d and tbl_implantations.nOrigin=%d",
        sync_number, origin);
}

int _db_sync_sqlite_implantation_open(db_sync_object_t *object, int origin, int sync_number)
{
    char text[1000];

    _db_sync_sql_implantation(text, origin, sync_number);
    return _db_sync_sqlite_prepare(text, object);
}*/

int Dbsync_implantation_sqlite::open(int origin, int sync_number)
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
    return Dbsync_sqlite::prepare(text, this);
}

/*int _db_sync_sqlite_implantation_close(db_sync_object_t *object)
{
    sqlite3_stmt *stmt = (sqlite3_stmt*) object->data;

    assert(object->type == DB_SYNC_OBJECT_IMPLANTATION);

    sqlite3_finalize(stmt);
    db_sync_implantation_clear_data(object->container);
    db_sync_object_clear_data(object);

    return 0;
}*/

int Dbsync_implantation_sqlite::close()
{
    sqlite3_stmt *stmt = (sqlite3_stmt*) data;

    assert(type == DB_SYNC_OBJECT_IMPLANTATION);

    sqlite3_finalize(stmt);

    clear_data();

    return 0;
}

/*int _db_sync_sqlite_implantation_next(db_sync_object_t *object)
{
    int rc;
    sqlite3_stmt *stmt = (sqlite3_stmt*) object->data;
    db_sync_implantation_t *implantation = (db_sync_implantation_t*) object->container;

    rc = _db_sync_sqlite_step(object);
    if (rc < 0)
        return rc;

    db_sync_implantation_clear_data(object);
    db_sync_object_clear_data(object);
    if (rc == DB_SYNC_EOF)
        return DB_SYNC_EOF;

    implantation->id = sqlite3_column_int(stmt, 0);
    implantation->patient_id = sqlite3_column_int(stmt, 1);
    if (implantation->patient_id == 516) {
        implantation->patient_id = sqlite3_column_int(stmt, 1);
    }
    implantation->unique_patient_id = strdup((char*) sqlite3_column_text(stmt, 2));
    implantation->pacemaker_model = sqlite3_column_int(stmt, 3);
    implantation->pacemaker_serial = sqlite3_column_int(stmt, 4);

    implantation->object.record_timestamp = strdup((char*) sqlite3_column_text(stmt, 5));
    implantation->object.sync_number = sqlite3_column_int(stmt, 6);
    implantation->object.origin = sqlite3_column_int(stmt, 7);

    return DB_SYNC_OK;
}*/

int Dbsync_implantation_sqlite::next()
{
    int rc;
    sqlite3_stmt *stmt = (sqlite3_stmt*)data;

    rc = Dbsync_sqlite::step(this);
    if (rc < 0)
        return rc;

    clear_data();

    if (rc == DB_SYNC_EOF)
        return DB_SYNC_EOF;

    id = sqlite3_column_int(stmt, 0);
    patient_id = sqlite3_column_int(stmt, 1);
    if (patient_id == 516) {
        patient_id = sqlite3_column_int(stmt, 1);
    }
    unique_patient_id = strdup((char*) sqlite3_column_text(stmt, 2));
    pacemaker_model = sqlite3_column_int(stmt, 3);
    pacemaker_serial = sqlite3_column_int(stmt, 4);

    record_timestamp = strdup((char*) sqlite3_column_text(stmt, 5));
    sync_number = sqlite3_column_int(stmt, 6);
    origin = sqlite3_column_int(stmt, 7);

    return DB_SYNC_OK;
}

/*int _db_sync_sql_implantation_insert(char *sql, db_sync_implantation_t *implantation)
{
    return sprintf(sql, "insert into tbl_implantations (nPatientID, nPacemakerModel, \
                    nPacemakerSerial, dtRecordTimestamp, nSyncNumber, nOrigin) values \
                   ((select nID from tbl_patients where strUniqueID=\"%s\"), %d, \
                   %d, \"%s\", %d, %d)", implantation->unique_patient_id,
        implantation->pacemaker_model, implantation->pacemaker_serial,
        implantation->object.record_timestamp, implantation->object.sync_number,
        implantation->object.origin);
}

int _db_sync_sql_implantation_update(char *sql, db_sync_implantation_t *implantation)
{
    return sprintf(sql, "update tbl_implantations set nPatientID= \
                   (select nID from tbl_patients where strUniqueID=\"%s\"), dtRecordTimestamp=\"%s\", \
                   nSyncNumber=%d, nOrigin=%d where nPacemakerModel=%d and nPacemakerSerial=%d",
        implantation->unique_patient_id, implantation->object.record_timestamp,
        implantation->object.sync_number, implantation->object.origin,
        implantation->pacemaker_model, implantation->pacemaker_serial);
}

int _db_sync_sqlite_implantation_add(db_sync_object_t *object)
{
    char text[1000];
    int rc;
    db_sync_implantation_t *implantation = (db_sync_implantation_t *) object->container;

    // сперва пробуем обновить
    sprintf(text, "select nID from tbl_implantations where \
            nPacemakerModel=%d and nPacemakerSerial=%d",
        implantation->pacemaker_model, implantation->pacemaker_serial);
    _db_sync_sqlite_prepare(text, object);
    rc = _db_sync_sqlite_step(object);
    sqlite3_finalize((sqlite3_stmt*) object->data);
    if (rc < 0)
        return rc;
    if (rc == DB_SYNC_OK) {
        _db_sync_sql_implantation_update(text, implantation);
        rc = sqlite3_exec((sqlite3*) object->sync_data->data_source, text,
            NULL, NULL, NULL);
        if (rc)
            return zerror_set(ERROR_DB, object->sync_data, text);
    } else {
        _db_sync_sql_implantation_insert(text, implantation);
        rc = sqlite3_exec((sqlite3*) object->sync_data->data_source, text, NULL,
            NULL, NULL);
        if (rc)
            return zerror_set(ERROR_DB, object->sync_data, text);
    }
    return 0;
}*/

int Dbsync_implantation_sqlite::add()
{
    char text[1000];
    int rc;

    // сперва пробуем обновить
    sprintf(text, "select nID from tbl_implantations where \
            nPacemakerModel=%d and nPacemakerSerial=%d",
            pacemaker_model, pacemaker_serial);
    Dbsync_sqlite::prepare(text, this);
    rc = Dbsync_sqlite::step(this);
    sqlite3_finalize((sqlite3_stmt*) data);
    if (rc < 0)
        return rc;
    if (rc == DB_SYNC_OK) {
        sprintf(text, "update tbl_implantations set nPatientID= \
                    (select nID from tbl_patients where strUniqueID=\"%s\"), dtRecordTimestamp=\"%s\", \
                    nSyncNumber=%d, nOrigin=%d where nPacemakerModel=%d and nPacemakerSerial=%d",
                    unique_patient_id, record_timestamp, sync_number, origin,
                    pacemaker_model, pacemaker_serial);
        rc = sqlite3_exec((sqlite3*) sync_data->data_source, text,
                          NULL, NULL, NULL);
        if (rc)
            return ERROR_DATA(Dbsync_sqlite::error_code(), sync_data, text);
    } else {
        sprintf(text, "insert into tbl_implantations (nPatientID, nPacemakerModel, \
                    nPacemakerSerial, dtRecordTimestamp, nSyncNumber, nOrigin) values \
                    ((select nID from tbl_patients where strUniqueID=\"%s\"), %d, \
                    %d, \"%s\", %d, %d)", unique_patient_id, pacemaker_model, pacemaker_serial,
                    record_timestamp, sync_number, origin);
        rc = sqlite3_exec((sqlite3*) sync_data->data_source, text, NULL,
                          NULL, NULL);
        if (rc)
            return ERROR_DATA(Dbsync_sqlite::error_code(), sync_data, text);
    }
    return 0;
}


/*int _db_sync_sqlite_session_open(db_sync_object_t *object, int origin, int sync_number)
{
    char text[1000];

    _db_sync_sql_session(text, origin, sync_number);
    return _db_sync_sqlite_prepare(text, object);
}
int _db_sync_sql_session(char *sql, int origin, int sync_number)
{
    return sprintf(sql, "select tbl_sessions.nID, tbl_sessions.nPacemakerModel, \
                    tbl_sessions.nPacemakerSerial, \
                    tbl_sessions.nSessionType, tbl_sessions.nSessionData, \
                    tbl_sessions.dtSessionDate, \
                    tbl_sessions.dtRecordTimestamp, \
                    tbl_sessions.nSyncNumber, tbl_sessions.nOrigin \
                    from tbl_sessions where tbl_sessions.nSyncNumber>%d and tbl_sessions.nOrigin=%d",
        sync_number, origin);
}*/
int Dbsync_session_sqlite::open(int origin, int sync_number)
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
    return Dbsync_sqlite::prepare(text, this);

}

int Dbsync_session_sqlite::update()
{
    return 0;
}

/*int _db_sync_sqlite_session_close(db_sync_object_t *object)
{
    sqlite3_stmt *stmt = (sqlite3_stmt*) object->data;

    assert(object->type == DB_SYNC_OBJECT_SESSION);

    sqlite3_finalize(stmt);
    db_sync_session_clear_data(object->container);
    db_sync_object_clear_data(object);

    return 0;
}*/
int Dbsync_session_sqlite::close()
{
    sqlite3_stmt *stmt = (sqlite3_stmt*) data;

    assert(type == DB_SYNC_OBJECT_SESSION);

    sqlite3_finalize(stmt);
    clear_data();

    return 0;
}

/*int _db_sync_sqlite_session_next(db_sync_object_t *object)
{
    int rc;
    const void *data;
    sqlite3_stmt *stmt = (sqlite3_stmt*) object->data;
    db_sync_session_t *session = (db_sync_session_t*) object->container;

    rc = _db_sync_sqlite_step(object);
    if (rc < 0)
        return rc;

    db_sync_session_clear_data(object);
    db_sync_object_clear_data(object);
    if (rc == DB_SYNC_EOF)
        return DB_SYNC_EOF;

    session->id = sqlite3_column_int(stmt, 0);
    session->model = sqlite3_column_int(stmt, 1);
    session->serial = sqlite3_column_int(stmt, 2);
    session->type = sqlite3_column_int(stmt, 3);
    // такой порядок прописан в мануале
    data = sqlite3_column_blob(stmt, 4);
    session->length = sqlite3_column_bytes(stmt, 4);
    session->data = malloc(session->length);
    memcpy(session->data, data, session->length);
    sscanf((char*) sqlite3_column_text(stmt, 5), "%d-%d-%d %d:%d:%d", &session->date.tm_year, &session->date.tm_mon,
        &session->date.tm_mday, &session->date.tm_hour, &session->date.tm_min, &session->date.tm_sec);

    session->object.record_timestamp = strdup((char*) sqlite3_column_text(stmt, 6));
    session->object.sync_number = sqlite3_column_int(stmt, 7);
    session->object.origin = sqlite3_column_int(stmt, 8);

    return DB_SYNC_OK;
}*/
int Dbsync_session_sqlite::next()
{
    int rc;
    const void *data;
    sqlite3_stmt *stmt = (sqlite3_stmt*) this->data;

    rc = Dbsync_sqlite::step(this);
    if (rc < 0)
        return rc;

    clear_data();

    if (rc == DB_SYNC_EOF)
        return DB_SYNC_EOF;

    id = sqlite3_column_int(stmt, 0);
    model = sqlite3_column_int(stmt, 1);
    serial = sqlite3_column_int(stmt, 2);
    session_type = sqlite3_column_int(stmt, 3);
    // такой порядок прописан в мануале
    data = sqlite3_column_blob(stmt, 4);
    length = sqlite3_column_bytes(stmt, 4);
    session_data = new unsigned char [length];
    memcpy(session_data, data, length);
    sscanf((char*) sqlite3_column_text(stmt, 5), "%d-%d-%d %d:%d:%d", &date.tm_year, &date.tm_mon,
           &date.tm_mday, &date.tm_hour, &date.tm_min, &date.tm_sec);

    record_timestamp = strdup((char*) sqlite3_column_text(stmt, 6));
    sync_number = sqlite3_column_int(stmt, 7);
    origin = sqlite3_column_int(stmt, 8);

    return DB_SYNC_OK;
}


/*int _db_sync_sqlite_session_add(db_sync_object_t *object)
{
    char text[1000];
    int rc;
    db_sync_session_t *session = (db_sync_session_t*) object->container;

    _db_sync_sql_session_insert(text, session);
    rc = _db_sync_sqlite_prepare(text, object);
    if (rc < 0)
        return -1;
    // привязываем данные
    rc = sqlite3_bind_blob((sqlite3_stmt*) object->data, 1, session->data,
        session->length, SQLITE_STATIC);
    if (rc < 0)
        return -1;
    rc = _db_sync_sqlite_step(object);
    sqlite3_finalize((sqlite3_stmt*) object->data);

    if (rc == DB_SYNC_EOF)
        return 0;

    return -1;
}
int _db_sync_sql_session_insert(char *sql, db_sync_session_t *session)
{
    char date[20];

    sprintf(date, "%04d-%02d-%02d %02d:%02d:%02d", session->date.tm_year, session->date.tm_mon, session->date.tm_mday,
        session->date.tm_hour, session->date.tm_min, session->date.tm_sec);
    return sprintf(sql, "insert into tbl_sessions (nPacemakerModel, \
                   nPacemakerSerial, nSessionType, \
                   nSessionData, dtSessionDate, dtRecordTimestamp, \
                   nSyncNumber, nOrigin) values \
                   (%d, %d, %d, ?, \"%s\", \"%s\", %d, %d)",
        session->model, session->serial, session->type,
        date, session->object.record_timestamp,
        session->object.sync_number,
        session->object.origin);
}*/
int Dbsync_session_sqlite::add()
{
    char text[1000];
    int rc;

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


    rc = Dbsync_sqlite::prepare(text, this);
    if (rc < 0)
        return -1;
    // привязываем данные
    rc = sqlite3_bind_blob((sqlite3_stmt*) this->data, 1, session_data,
        length, SQLITE_STATIC);
    if (rc < 0)
        return -1;
    rc = Dbsync_sqlite::step(this);
    sqlite3_finalize((sqlite3_stmt*) this->data);

    if (rc == DB_SYNC_EOF)
        return 0;

    return -1;
}

Dbsync_sqlite::~Dbsync_sqlite()
{
    close_source();
}
