#ifndef DBSYNC_ENGINE_H
#define DBSYNC_ENGINE_H

#include <memory>
#include <vector>
#include <time.h>

#define DB_SYNC_DIRECTION_TO          1
#define DB_SYNC_DIRECTION_FROM        2
#define DB_SYNC_DIRECTION_BOTH        3

#define DB_SYNC_ERROR       -1
#define DB_SYNC_OK          0
#define DB_SYNC_EOF         1

enum
{
    DB_SYNC_OBJECT,
    DB_SYNC_OBJECT_ORIGIN,
    DB_SYNC_OBJECT_PATIENT,
    DB_SYNC_OBJECT_IMPLANTATION,
    DB_SYNC_OBJECT_SESSION,
    DB_SYNC_OBJECT_LAST = DB_SYNC_OBJECT_SESSION,

    DB_SYNC_OBJECTS_NUMBER
};

// если меняем, отредактировать db_sync_object_clear_data()
//struct db_sync_object_s
//{
//    int type;
//    int sync_number;
//    int origin;
//    char *record_timestamp; // делаем текстовым, чтобы можно было читать прям из базы глазами
//    void *container;
//    void *data;
//    db_sync_t *sync_data;
//    int (*open)(db_sync_object_t *object, int origin, int sync_number);
//    int (*close)(db_sync_object_t *object);
//    int (*next)(db_sync_object_t *object);
//    int (*add)(db_sync_object_t *object);
//    void (*copy_data)(db_sync_object_t *dst_object, db_sync_object_t *src_object);
//    void (*clear_data)(db_sync_object_t *object);
//};
struct Dbsync;

struct Dbsync_object
{
    Dbsync_object()
        : type{DB_SYNC_OBJECT}, sync_number{0}, origin{0}, record_timestamp{nullptr}, data{nullptr}, sync_data{nullptr}
    {}
    virtual ~Dbsync_object()
    {
        clear_data();
    }
    virtual int open(int, int){return 0;}
    virtual int close()
    {
        this->clear_data();
        return 0;
    }
    virtual int next(){return 0;}
    virtual int add(){return 0;}
    virtual void copy_data(std::unique_ptr<Dbsync_object> &from);
    virtual void clear_data();

    int type;
    int sync_number;
    int origin;
    char *record_timestamp; // делаем текстовым, чтобы можно было читать прям из базы глазами
    void *data;
    Dbsync *sync_data;
};

struct db_sync_statistics
{
    std::vector<int> objects_added = std::vector<int>(DB_SYNC_OBJECTS_NUMBER);
    std::vector<int> objects_readed = std::vector<int>(DB_SYNC_OBJECTS_NUMBER);

    void clear()
    {
        for(int i = 0; i < DB_SYNC_OBJECTS_NUMBER; i++) {
            objects_added[i] = 0;
            objects_readed[i] = 0;
        }
    }
};

//struct db_sync_origin_s
//{
//    db_sync_object_t object;
//    char *origin_string;
//    int (*open)(db_sync_origin_t *origin, char *origin_id);
//    int (*update)(db_sync_origin_t *origin, int sync_number);
//};

struct Dbsync_origin : public Dbsync_object
{
    Dbsync_origin() : origin_string{nullptr}
    {
        type = DB_SYNC_OBJECT_ORIGIN;
    }
    virtual int open(const char *) {return 0;}
    virtual int update(int) {return 0;}
    virtual void clear_data();

    char *origin_string;
    db_sync_statistics statistics;

    int sync_to(Dbsync_origin *dst);
};

//typedef struct
//{
//    db_sync_object_t object;
//    int id;
//    int patient_id;
//    char *unique_patient_id;
//    int pacemaker_model;
//    int pacemaker_serial;
//} db_sync_implantation_t;

struct Dbsync_implantation : public Dbsync_object
{
    Dbsync_implantation()
        : id{0}, patient_id{0}, unique_patient_id{nullptr}, pacemaker_model{0}, pacemaker_serial{0}
    {
        type = DB_SYNC_OBJECT_IMPLANTATION;
    }
    int id;
    int patient_id;
    char *unique_patient_id;
    int pacemaker_model;
    int pacemaker_serial;

    // Dbsync_object interface
public:
    void copy_data(std::unique_ptr<Dbsync_object> &from);
    void clear_data();
};

//typedef struct
//{
//    db_sync_object_t object;
//    int id;
//    char *first_name;
//    char *last_name;
//    char *patronym;
//    char *address;
//    struct tm birth_date;
//    char *unique_id;
//    unsigned char sex;
//} db_sync_patient_t;

struct Dbsync_patient : public Dbsync_object
{
    Dbsync_patient() :
        first_name{nullptr}, last_name{nullptr}, patronym{nullptr}, address{nullptr}, unique_id{nullptr}, id{0}
    {
        type = DB_SYNC_OBJECT_PATIENT;
    }

//    virtual int open(char *) {return 0;}
    virtual int update() {return 0;}
    virtual void clear_data();
    virtual void copy_data(std::unique_ptr<Dbsync_object> &from);

    char *first_name;
    char *last_name;
    char *patronym;
    char *address;
    char *unique_id;
    int id;
    struct tm birth_date;
    int sex;
};

//typedef struct
//{
//    db_sync_object_t object;
//    int id;
//    struct tm date;
//    int model;
//    int serial;
//    int type;
//    int length;
//    unsigned char *data;
//} db_sync_session_t;

struct Dbsync_session : public Dbsync_object
{
    Dbsync_session() :
        id{0}, model{0}, serial{0}, session_type{0}, length{0}, session_data{nullptr}
    {
        type = DB_SYNC_OBJECT_SESSION;
    }

    virtual int update() {return 0;}
    virtual void clear_data();
    virtual void copy_data(std::unique_ptr<Dbsync_object> &from);

    int id;
    struct tm date;
    int model;
    int serial;
    int session_type;
    int length;
    unsigned char *session_data;
};

//struct db_sync_s {
//    db_sync_origin_t *origin;
//    db_sync_object_t **objects;
//    // если не ноль, объект не является временной БД для синхронизации
//    // соддержит уникальный id базы
//    //char *local_origin_string;
//    // последняя ошибка
//    //char *error;
//    void *data_source;
//    int (*increment_sync)(db_sync_t *sync);
//    int (*step)();     // long operation step callback
//    int (*trans_begin)(db_sync_t *sync);
//    int (*trans_commit)(db_sync_t *sync);
//    int (*trans_rollback)(db_sync_t *sync);
//    struct db_sync_statistics statistics;
//};

struct Dbsync
{
    struct Callback
    {
        virtual void step(){}
    };

    Dbsync(Dbsync_origin *origin, Callback* callback = new Callback());
    std::unique_ptr<Dbsync_origin> origin;
    std::vector<std::unique_ptr<Dbsync_object>> objects;
    void *data_source;

    virtual int increment_sync() = 0;
//    virtual int step(){return 0;}     // long operation step callback
    virtual int trans_begin() = 0;
    virtual int trans_commit() = 0;
    virtual int trans_rollback() = 0;

    virtual int open_source() = 0;
    virtual int close_source() = 0;

    // sync одинаковая для всех, переместил сюда
    // для скорости не используем умные указатели
    int sync(Dbsync *other, char direction);
    int sync_to(Dbsync *dst);
    int copy_origins(Dbsync *dst);

    void set_callback(Callback * c) { m_callback = c; }

    struct db_sync_statistics statistics;

private:
    int sync_objects_to(Dbsync *dst);
    int do_step(Dbsync *dst);

    Callback* m_callback;
};

#endif // DBSYNC_ENGINE_H
