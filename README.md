# swl, a flexible ETL

You define pipelines that swl will run. All of them are run in order.

It is transactionnal : on sources and sinks that support it, everything is run in a transaction that will only be commited once the script is done.

# Why

I wanted to be able to move data between different sources and sinks in simple command lines for simple cases and yet still have the flexibility to do some much more complex operations.

Also be able to still use sql in some cases !

## As a command-line tool

swl sqlite://myfile.db :: all_tables :: stdout
     Adapter :: Adapter -> Stream :: Stream -> Sink

```
swl <<EOF

# table is named "pouet"
csv://pouet.csv
  :: sqlite://:tmp?autocreate

---

csv://pouet2.csv
  :: after_table create index on pouet2(id, date)
  :: sqlite://:tmp?autocreate

;;

sqlite://:tmp
  select p1.*, p2.* from pouet p1 inner join pouet2 p2 on p1.id = p2.source_id as zobi
  :: upsert on id
  :: postgres://app:app@1.1.1.1/app?autocreate

;;

sqlite://myfile.db users | targets | user_targets
  :: [ collection == users ? before_collection delete from haha where date <= current_date ]
  :: postgres://app:app@1.1.1.1/app

xlsx://haha.xlsx?sanitize_names

json://mydata {a: 1, b:2} {a: 2, b: 3} {a: 5, b: 6}

rison://mydata (a:1,b:2) (a:2,b:2) (a:2,b:4)

# This may crash, as the topology that will be infered here won't be the corect one
rison://mycollection a:1,b:2 a:2,b:4 a:5,b:6 a:toto,b:tata
  :: postgres://app:app@1.1.1.1/app
```
```
$pg = postgres://app:app@1.1.1.1/app ;;
$sq = sqlite://loading.db ;;

$sq users | targets | user_targets :: truncate :: $pg
```

Only one read source may be active at a time (?)

Brutal loading :

`swl sqlite://myfile.db users | targets | user_targets :: truncate :: postgres://app:app@1.1.1.1/app`

## As a library



# Principles


# Extending swl
