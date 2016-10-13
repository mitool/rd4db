package main

import (
	"flag"
	"math"

	"github.com/admpub/confl"
	"github.com/admpub/log"
	"github.com/webx-top/db"
	"github.com/webx-top/db/lib/factory"
	"github.com/webx-top/db/mongo"
	"github.com/webx-top/db/mysql"
	"gopkg.in/mgo.v2/bson"
)

type CateItem struct {
	ID   string `bson:"id" db:"id"`
	Name string `bson:"name" db:"name"`
}

type CateModel struct {
	First  *CateItem `bson:"first" db:"first"`
	Second *CateItem `bson:"second" db:"second"`
	Third  *CateItem `bson:"third" db:"third"`
}

type ContentModel struct {
	ID    string     `bson:"id" db:"id"`
	Title string     `bson:"title" db:"title"`
	Cate  *CateModel `bson:"cate" db:"cate"`
}

type EventModel struct {
	ID        bson.ObjectId `bson:"_id" db:"_id"`
	Event     string        `bson:"event" db:"event"`
	Timestamp uint          `bson:"timestamp" db:"timestamp"`
	Content   *ContentModel `bson:"content" db:"content"`
	Udid      string        `bson:"udid" db:"udid"`
	Platform  string        `bson:"platform" db:"platform"`
	OS        string        `bson:"os" db:"os"`
	OsType    string        `bson:"osType" db:"osType"`
	Version   string        `bson:"version" db:"version"`
	BundleId  string        `bson:"bundleId" db:"bundleId"`
	IP        string        `bson:"ip" db:"ip"`
	Account   struct {
		ID string `bson:"accountId" db:"accountId"`
	} `bson:"account" db:"account"`
}

func main() {
	configFile := flag.String(`c`, `dbconfig.yml`, `database setting`)
	flag.Parse()

	log.Sync()
	log.DefaultLog.AddSpace = true
	log.SetFatalAction(log.ActionExit)

	dbConfig := struct {
		Mongo mongo.ConnectionURL
		MySQL mysql.ConnectionURL
	}{}

	_, err := confl.DecodeFile(*configFile, &dbConfig)
	if err != nil {
		log.Fatal(err)
	}

	dbMongo, err := mongo.Open(dbConfig.Mongo)
	if err != nil {
		log.Fatal(err)
	}

	dbMySQL, err := mysql.Open(dbConfig.MySQL)
	if err != nil {
		log.Fatal(err)
	}

	cluster := factory.NewCluster().AddW(dbMongo)
	factory.AddCluster(cluster) //第一次添加。索引编号为0
	clusterMySQL := factory.NewCluster().AddW(dbMySQL)
	factory.AddCluster(clusterMySQL) //第二次添加。索引编号为1，以此类推。
	//factory.SetDebug(true) //调试时可以打开Debug模式来查看sql语句
	defer factory.CloseAll()

	//使用Link(1)来选择索引编号为1的数据库连接(默认使用编号为0的连接)
	result := factory.NewParam().Setter().Link(1).C(`libuser_detail`).Result()

	detail := map[string]string{}
	for result.Next(&detail) {
		if len(detail["appid"]) == 0 {
			continue
		}
		log.Info(`AppID`, detail["appid"])

		mdt := new([]EventModel)
		cond := db.Cond{
			"udid":     "00old00analysis00",
			"event IN": []string{"downloadMag", "downloadBook"},
		}
		size := 1000
		page := 1

		//这里没有使用Link()函数，默认选择索引编号为0的数据库连接
		cnt, err := factory.NewParam().Setter().C(`event` + detail["appid"]).Args(cond).Page(page).Size(size).Recv(mdt).List()
		if err != nil {
			if err == db.ErrNoMoreRows {
				log.Error(err)
				continue
			}
			log.Fatal(err)
		}
		tot := cnt()
		pages := int(math.Ceil(float64(tot) / float64(size)))
		for ; page <= pages; page++ {
			if page > 1 {
				_, err = factory.NewParam().Setter().C(`event` + detail["appid"]).Args(cond).Page(page).Size(size).Recv(mdt).List()
				if err != nil {
					if err == db.ErrNoMoreRows {
						log.Error(err)
						break
					}
					log.Fatal(err)
				}
			}
			for _, row := range *mdt {
				n, err := factory.NewParam().Setter().C(`event` + detail["appid"]).Args(db.Cond{
					"_id <>":            row.ID,
					"timestamp":         row.Timestamp,
					"account.accountId": row.Account.ID,
				}).Count()
				if err == nil && n > 0 {
					log.Infof(`Found %d duplicate(s) => %s`, n, row.ID)
					err = factory.NewParam().Setter().C(`event` + detail["appid"]).Args(db.Cond{"_id": row.ID}).Delete()
					if err == nil {
						log.Info(`Remove success.`)
					}
				}
				if err != nil {
					if err == db.ErrNoMoreRows {
						log.Error(err)
						break
					}
					log.Fatal(err)
				}
			}
		}
	}
	result.Close()

}
