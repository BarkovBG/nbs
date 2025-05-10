# Демонстрация

## Аргументация, почему изменения, сделанные для демонстрации, не имеет смысл коммитить в репозиторий.
Изменения для создания демонстрации были реализованы локально и коммитить их в
репозиторий не имеет смысла, так как:
- Реальные задачи DM предполагают наличие работающего NBS для обслуживания дисков,
так как реальное использование разработанной функциональности предполагается в задачах, например,
создания резервной копии из дисков пользователей.
- Поднятие NBS в свою очередь требует поднятие YDB BlobStorage, Hive, SS, NodeBroker
и других компонент. Поднятие подобной инфраструктуры нецелесообразно для демонстрации работы.

Итак, для демонстрации были сделаны изменения:
1) Доработка BlankTask DM для использования s3. Коммитить изменения не имеет смысла, тк в
prod BlankTask используется для нагрузочного тестированя.
2) Изменена авторизация в БД YDB для походов в личную базу, которую я развернул в облаке.
3) Изменена авторизация в s3 на анонимную, чтобы в демонстрации можно было легко показать работу функционала.

**Резюмируя, были сделанны изменения, которые облечают демонстрацию работы и ее улучшают ее понимание. Но в PROD решении они не имеют смысла.**

## Аргументация, почему не производится локальный запуск.
- Локальный запуск не производится, так как появляются зависимости на YDB, S3 и прочее.
- Это сложная инфраструктура, доступность которой может нарушаться.
- Для демонстрации необходимо иметь 2 разных конфига сервера и ряд локальных изменений. Запуск такой сложной системы в режиме online может быть долгим и сложным.
- При локальном запуске нельзя не обговорить все тонкости настроек системы. Например, ограничение на кол-во задач определенного типа на хосте для демонстрации 2 вводится для того, чтобы продемонстрировать ограничение на количество хостов, где блокируется исполнение задачи с недоступной компонентой.
**Углубляться в подобные тонконсти системы на защите не хочется в связи с ограничением по времени**
- **Запись демонстрации производилась несколькими дублями, чтобы в краткое время показать все тонкости системы. Так как система распределенная, могут происходить гонки, при которых система будет работать правильно, но понять ее работу будет намного сложнее.**

## Необходимая инфраструктура для демонстрации

Итак, необходимым для демонстрации является:
1) Развернутая YDB в облаке
![alt text](<Снимок экрана 2025-05-10 в 16.47.19.png>)
2) Развернутый S3 в облаке
![alt text](<Снимок экрана 2025-05-10 в 16.47.37.png>)
3) Виртуальная машина в облаке на Linux
![alt text](<Снимок экрана 2025-05-10 в 16.46.38.png>)

## Подготовка к демонстрации

1) Для тестирования функциональности нужно собрать проект, а именно бинарь
disk_manager, который находится по пути `cloud/disk_manager/cmd/disk-manager`:
```
ya make
```
А также disk-manager-admin для создания новых задач по пути `cloud/disk_manager/cmd/disk-manager-admin`:
```
ya make
```
2) После этого появится возможность запускать сервис с конфигом:
```
./disk-manager --config /home/barkovbg/diplom/server-config.txt
```
3) Для того, чтобы disk_manager работал - ему нужна база данных ydb. Ее можно
(и я так сделал для демонстрации) развернуть в Яндекс.Облаке.
Так же для прохождения авторизации нужно получить токен:
```
ydb -e grpcs://ydb.serverless.yandexcloud.net:2135 --yc-token-file ~/diplom/token -d /ru-central1/b1g06bpp2fj1ve048589/etn1ak8gsoc82h42o5nv auth get-token -f
```
Во флагах нужно указать необходимые параметры базы (в том числе токен yandex cloud).
Больше можно почитать в документации YDB.

## Конфиг сервера, который использовался для демонстрации 1:
```
GrpcConfig: {
    Port: 9797
    Hostname: "server1"
    Insecure: true
    KeepAlive: {}
}
TasksConfig: {
    RegularSystemTasksEnabled: false
    ComponentsByTaskTypes: {
        key: "tasks.Blank"
        value: {
            Components: "s3"
        }
    }
}
NbsConfig: {
    GrpcKeepAlive: {}
}
DisksConfig: {
}
NfsConfig: {
}
FilesystemConfig: {
}
PoolsConfig: {
}
ImagesConfig: {
}
SnapshotsConfig: {
}
LoggingConfig: {
    LoggingStderr: {}
    Level: LEVEL_ERROR
}
MonitoringConfig: {
    RestartsCountFile: "/home/barkovbg/diplom/restarts-count.txt"
    ServerVersionFile: "/home/barkovbg/diplom/server-version.txt"
}
AuthConfig: {
    DisableAuthorization: true
}
PersistenceConfig: {
    Endpoint: "ydb.serverless.yandexcloud.net:2135"
    Database: "ru-central1/b1g06bpp2fj1ve048589/etn1ak8gsoc82h42o5nv"
    Secure: true
    DisableAuthentication: true
}
```

## Конфиг сервера, который использовался для демонстрации 2:
```
GrpcConfig: {
    Port: 9797
    Hostname: "server1"
    Insecure: true
    KeepAlive: {}
}
TasksConfig: {
    RegularSystemTasksEnabled: false
    InflightTaskPerNodeLimits: {
        key: "tasks.Blank"
        value: 1
    }
    ComponentsByTaskTypes: {
        key: "tasks.Blank"
        value: {
            Components: "s3"
        }
    }
}
NbsConfig: {
    GrpcKeepAlive: {}
}
DisksConfig: {
}
NfsConfig: {
}
FilesystemConfig: {
}
PoolsConfig: {
}
ImagesConfig: {
}
SnapshotsConfig: {
}
LoggingConfig: {
    LoggingStderr: {}
    Level: LEVEL_ERROR
}
MonitoringConfig: {
    RestartsCountFile: "/home/barkovbg/diplom/restarts-count.txt"
    ServerVersionFile: "/home/barkovbg/diplom/server-version.txt"
}
AuthConfig: {
    DisableAuthorization: true
}
PersistenceConfig: {
    Endpoint: "ydb.serverless.yandexcloud.net:2135"
    Database: "ru-central1/b1g06bpp2fj1ve048589/etn1ak8gsoc82h42o5nv"
    Secure: true
    DisableAuthentication: true
}
```
## Выполнение демонстрации:
1) Запуск сервера:
```
HOST=server1 YDB_TOKEN=<YOUR_YDB_TOKEN> ./disk-manager --config "/home/barkovbg/diplom/server-config.txt"
```
**При запуске сервера могут быть логи, которых нет в демонстрации из-за обновления версии YDB GO SDK. А так же для лучшей демонстрации некоторые логи отключались вручную.**

2) Создание задач:
```
YDB_TOKEN=<YOUR_YDB_TOKEN> ./disk-manager-admin tasks schedule --config ~/diplom/client-config.txt --server-config ~/diplom/server-config.txt
```
