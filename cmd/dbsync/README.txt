Debian Package Build
------------------------------------------------------
dpkg -b ./dpkg/ dbsync.deb

Profiling:
------------------------------------------------------
1. profil erstellen mit profile.sh
2. profil betrachten (z.B. vergleich) go tool pprof -base 1.heap 10.heap (siehe https://github.com/google/pprof)


CLI Examples:
------------------------------------------------------

SQLServer:
DEV (ml camp)
./dbsync -a db_sync -c ml_camp -ct zivxwwu4N3bd_bT76iDEXJIx0T64UnQCOimyXrB4zgu-1bxW6U3cnW_D52KRpZjKSmva9akS-pbFsZ8le6BnHFAX9LiQKFbfefNCgWUWmPLg-1 -url "sqlserver://sa:modimai1.Sfm@localhost:1433/df_ml_camp"
STABLE:
cloudit/showcase INIT: ./dbsync -m database -c 2Z8SZLZM5WDWV3UG -ct BKU-s6XEFKNbW4QaioV-IEKZb0CSZQWvCPGDQMWSHpNfbvXHAP7RNO3ESgw0nyNs0_8NG6KhoSjwRjiyatBjOtl0ZGtoz0 -dbms sqlserver -dburi "sqlserver://sa:modimai1.Sfm@localhost:1433?database=df_cloudit_showcase" -dbmode db_init

MySQL:
DEV (ml camp)
./dbsync -a db_sync -c ml_camp -ct zivxwwu4N3bd_bT76iDEXJIx0T64UnQCOimyXrB4zgu-1bxW6U3cnW_D52KRpZjKSmva9akS-pbFsZ8le6BnHFAX9LiQKFbfefNCgWUWmPLg-1 -url "mysql://root:modimai1.Sfm@localhost:3306/df_ml_camp"
STABLE (Showcase)
./dbsync -a db_sync -c 2Z8SZLZM5WDWV3UG -ct BKU-s6XEFKNbW4QaioV-IEKZb0CSZQWvCPGDQMWSHpNfbvXHAP7RNO3ESgw0nyNs0_8NG6KhoSjwRjiyatBjOtl0ZGtoz0 -url "mysql://root:modimai1.Sfm@localhost:3306/df_cloudit_showcase"

Postgres:
DEV (ml camp)
./dbsync -a db_sync -c ml_camp -ct zivxwwu4N3bd_bT76iDEXJIx0T64UnQCOimyXrB4zgu-1bxW6U3cnW_D52KRpZjKSmva9akS-pbFsZ8le6BnHFAX9LiQKFbfefNCgWUWmPLg-1 -url "postgres://postgres:modimai1.Sfm@localhost:5432/df_ml_camp"
STABLE (gevekom/hello you service)
 ./dbsync -a db_sync -c 9AEAHFKENNLSR6YR -ct BKU-s6XEFKNbW4QaioV-IEKZb0CSZQWvCPGDQMWSHpNfbvXHAP7RNO3ESgw0nyNs0_8NG6KhoSjwRjiyatBjOtl0ZGtoz0 -url "postgres://postgres:modimai1.Sfm@localhost:5432/df_hello_you"


Webhook:
DEV (Inbound Test)
./dbsync -a webhook -c V5L7SF6WAF8CK34J -ct D5d4GatugBC6E4nveVA0cLfa5tpiLlySOxX-Qn8wcXFv7AsTs8TtZAb12-IPVq8WWkIe-tXyXdh380CKr_JNrs9TKMYf72url 'https://test-dot-appstackfive.appspot.com/!sjq2C9_qAYIlC3WDAbSX-8kGx0IdacJ43FfR67IGoMck8UrMKgHxcZMETK_5vMYgJQ2dig66bxLj1wz5dWX3w3qROj0/api/whtest/transactions'



Query Examples:
------------------------------------------------------
------------------------------------------------------


SQLServer:
------------------------------------------------------
Successful Calls by date (cloudit showcase)
------------------------------------------------------
use df_cloudit_showcase;
select
    SUBSTRING(fired, 0, 11) as date,
    user_loginname as login,
    status,
    count(*) as count
from 
    df_transactions
where 
    user_loginname = 'MLiebschner'
    and status = 'success'
group by 
    SUBSTRING(fired, 0, 11), user_loginname, status
order by date desc

------------------------------------------------------
Edittime hours by date (cloudit showcase)
------------------------------------------------------
use df_cloudit_showcase;

select 
    SUBSTRING(fired, 0, 11) as date,
    user_loginname as login,
    ROUND(SUM(edit_time_sec) / 3600.0, 2) as editTime_hour
from 
    df_transactions
where user_loginname = 'MLiebschner'
group by 
    SUBSTRING(fired, 0, 11), user_loginname
order by date desc

------------------------------------------------------
------------------------------------------------------



Postgres:
------------------------------------------------------
Successful Calls by date (gevekom hello you service)
------------------------------------------------------
select
	substring(fired, 0, 11) as day,
	task,
	count(*) as success,
	"user_loginName"
from
	public.df_transactions
where
	true
	and substring(task, 0, 4)= 'fc_'
	and "isHI" = true
	and status = 'success'
group by
	day,
	task,
	"user_loginName"
order by
	day desc,
	task asc,
	"user_loginName" asc