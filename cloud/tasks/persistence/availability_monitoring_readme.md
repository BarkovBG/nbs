1) Для тестирования функциональности нужно собрать проект, а именно бинарь
disk_manager, который находится по пути `cloud/disk_manager/cmd/disk-manager`:
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
Во влагах нужно указать необходимые параметры базы (в том числе токен yandex cloud).
Больше можно почитать в документации YDB.

4)
