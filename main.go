package main

import (
	"flag"
	"math"
	"strings"
	"sync"
	"time"

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

var config = struct {
	ConfigFile *string
	Operation  *string
}{}

func main() {
	config.ConfigFile = flag.String(`c`, `dbconfig.yml`, `database setting`)
	config.Operation = flag.String(`t`, `insertBrandId`, `operation type: removeDuplicates / updateEvent / updateOsType)`)
	flag.Parse()

	log.Sync()
	log.DefaultLog.AddSpace = true
	log.SetFatalAction(log.ActionExit)

	dbConfig := struct {
		Mongo mongo.ConnectionURL
		MySQL mysql.ConnectionURL
	}{}

	_, err := confl.DecodeFile(*config.ConfigFile, &dbConfig)
	if err != nil {
		log.Fatal(err)
	}
	mongo.ConnTimeout = time.Second * 30
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
	factory.SetDebug(true)           //调试时可以打开Debug模式来查看sql语句
	defer factory.CloseAll()

	detail := map[string]string{}
	detail["appid"] = "11244bf15870d8567b41d99b908544ed"

	wg := &sync.WaitGroup{}
	if _, ok := detail["appid"]; ok {
		wg.Add(1)
		go checkAppID(detail, wg)
	} else {
		//使用Link(1)来选择索引编号为1的数据库连接(默认使用编号为0的连接)
		result := factory.NewParam().Setter().Link(1).C(`libuser_detail`).Result()
		total, err := factory.NewParam().Setter().Link(1).C(`libuser_detail`).Count()
		if err != nil {
			log.Fatal(err)
		}
		wg.Add(int(total))
		for result.Next(&detail) {
			switch *config.Operation { //修改event值infoXXX为downloadXXX
			case `updateEvent`:
				go checkEvent(detail, wg)
			default:
				go checkAppID(detail, wg)
			}
		}
		result.Close()
	}
	wg.Wait()
}

type Executor struct {
	Cond db.Cond
	Func func(EventModel, map[string]string) error
}

var executors = map[string]*Executor{
	"updateOsType": &Executor{ //更新osType
		Cond: db.Cond{
			"udid":   "00old00analysis00",
			"osType": "windows",
		},
		Func: updateOsType,
	},
	"removeDuplicates": &Executor{ //删除重复数据
		Cond: db.Cond{"udid": "00old00analysis00"},
		Func: removeDuplicates,
	},
	"insertBrandId": &Executor{
		Cond: db.Cond{
			"content.bid $exists":  false,
			"content.cate $exists": false,
			"event IN": []string{
				"downloadMag",
				"infoMag",
			},
		},
		Func: insertBrandId,
	},
}

func checkAppID(detail map[string]string, wg *sync.WaitGroup) {
	defer wg.Done()
	if len(detail["appid"]) == 0 {
		return
	}
	log.Info(`AppID`, detail["appid"])

	mdt := new([]EventModel)
	cond := db.Cond{}
	executor, ok := executors[*config.Operation]
	if !ok {
		return
	}
	for k, v := range executor.Cond {
		cond[k] = v
	}
	size := 1000
	page := 1

	//这里没有使用Link()函数，默认选择索引编号为0的数据库连接
	cnt, err := factory.NewParam().Setter().C(`event` + detail["appid"]).Args(cond).Page(page).Size(size).Recv(mdt).List()
	if err != nil {
		if err == db.ErrNoMoreRows || factory.IsTimeoutError(err) {
			log.Error(err)
			return
		}
		log.Fatal(err)
	}
	tot := cnt()
	pages := int(math.Ceil(float64(tot) / float64(size)))
	for ; page <= pages; page++ {
		if page > 1 {
			_, err = factory.NewParam().Setter().C(`event` + detail["appid"]).Args(cond).Page(page).Size(size).Recv(mdt).List()
			if err != nil {
				if err == db.ErrNoMoreRows || factory.IsTimeoutError(err) {
					log.Error(err)
					break
				}
				log.Fatal(err)
			}
		}
		for _, row := range *mdt {
			err := executor.Func(row, detail)
			if err != nil {
				if err == db.ErrNoMoreRows || factory.IsTimeoutError(err) {
					log.Error(err)
					break
				}
				log.Fatal(err)
			}
		}
	}
}

//删除重复数据
func removeDuplicates(row EventModel, detail map[string]string) error {
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
	return err
}

//更新osType
func updateOsType(row EventModel, detail map[string]string) error {
	var osType, bundleId string
	switch row.Platform {
	case `pc`, `pc_down`:
		osType = `Windows`
		bundleId = `com.dooland.pc`
	case `ipad`:
		osType = `iOS`
		bundleId = `com.dooland.padforiosfromweb.reader`
	case `iphone`:
		osType = `iOS`
		bundleId = `com.dooland.mobileforiosfromweb.reader`
	case `android`:
		osType = `Android`
		bundleId = `com.dooland.padforandroidfromweb.reader`
	case `androidmobile`:
		osType = `Android`
		bundleId = `com.dooland.mobileforandroidfromweb.reader`
	case `waparticle`:
		osType = `Wap`
		bundleId = `com.dooland.wapforweb.reader`
	case `article`:
		osType = `Windows`
		bundleId = `com.dooland.pc`
	case `dudubao`:
		osType = `Dudubao`
		bundleId = `com.dooland.dudubao`
	case `dudubao_down`:
		osType = `Dudubao`
		bundleId = `com.dooland.dudubao`
	default:
		return nil
	}
	log.Infof(`Update [%s] %s => %s, %s => %s`, row.ID, row.OsType, osType, row.BundleId, bundleId)
	err := factory.NewParam().Setter().C(`event` + detail["appid"]).Args(db.Cond{"_id": row.ID}).Send(map[string]string{
		"osType":   osType,
		"bundleId": bundleId,
	}).Update()
	return err
}

//修改infoXXX为downloadXXX
func updateEvent(row EventModel, detail map[string]string) error {
	if strings.HasPrefix(row.Event, `info`) == false {
		return nil
	}
	event := `download` + strings.TrimPrefix(row.Event, `info`)
	log.Infof(`Update [%s] %s => %s, %s => %s`, row.ID, row.Event, event)
	err := factory.NewParam().Setter().C(`event` + detail["appid"]).Args(db.Cond{"_id": row.ID}).Send(map[string]string{
		"event": event,
	}).Update()
	return err
}

//修改infoXXX为downloadXXX
func checkEvent(detail map[string]string, wg *sync.WaitGroup) {
	defer wg.Done()
	if len(detail["appid"]) == 0 {
		return
	}
	log.Info(`AppID`, detail["appid"])

	size := 1000
	page := 1
	r := []map[string]string{}
	cnt, err := factory.NewParam().Setter().Link(1).C(`user_down_mag`).Args(db.Cond{"lib_id": detail["id"]}).Recv(&r).Page(page).Size(size).List()
	if err != nil {
		if err == db.ErrNoMoreRows || factory.IsTimeoutError(err) {
			log.Error(err)
			return
		}
		log.Fatal(err)
	}
	tot := cnt()
	pages := int(math.Ceil(float64(tot) / float64(size)))
	for ; page <= pages; page++ {
		if page > 1 {
			_, err = factory.NewParam().Setter().Link(1).C(`user_down_mag`).Args(db.Cond{"lib_id": detail["id"]}).Recv(&r).Page(page).Size(size).List()
			if err != nil {
				if err == db.ErrNoMoreRows || factory.IsTimeoutError(err) {
					log.Error(err)
					break
				}
				log.Fatal(err)
			}
		}
		for _, row := range r {
			t, err := time.Parse(`2006-01-02 15:04:05`, row["add_time"])
			if err != nil {
				log.Error(err)
				continue
			}
			mdt := new(EventModel)
			cond := db.Cond{
				"udid":              "00old00analysis00",
				"event IN":          []string{"infoMag", "infoBook"},
				"account.accountId": row["user_id"],
				"timestamp":         t.Unix(),
			}
			err = factory.NewParam().Setter().C(`event` + detail["appid"]).Args(cond).Page(page).Size(size).Recv(mdt).One()
			if err != nil {
				if err == db.ErrNoMoreRows || factory.IsTimeoutError(err) {
					log.Error(err)
					continue
				}
				log.Fatal(err)
			}
			err = updateEvent(*mdt, detail)
			if err != nil {
				if err == db.ErrNoMoreRows || factory.IsTimeoutError(err) {
					log.Error(err)
					continue
				}
				log.Fatal(err)
			}
		}
	}
}

//插入品牌ID
func insertBrandId(row EventModel, detail map[string]string) error {
	if len(row.Content.ID) == 0 {
		return nil
	}
	var brandId string
	recv := map[string]string{}
	err := factory.NewParam().Setter().Link(1).C(`dudubao.mag_list`).Args(db.Cond{"id": row.Content.ID}).Recv(&recv).One()
	if err != nil {
		if err == db.ErrNoMoreRows {
			err = factory.NewParam().Setter().Link(1).C(`dudubao_bak.mag_list_bak`).Args(db.Cond{"id": row.Content.ID}).Recv(&recv).One()
		}
	}
	if err != nil {
		return err
	}
	log.Infof(`Update [%s] %s => %s`, row.ID, row.Content.ID, recv["sort_id"])
	brandId = recv["sort_id"]
	if len(brandId) == 0 || brandId == `0` {
		log.Warn(` -> Skiped.`)
		return nil
	}
	err = factory.NewParam().Setter().C(`event` + detail["appid"]).Args(db.Cond{"_id": row.ID}).Send(map[string]string{
		"content.bid": brandId,
	}).Update()
	return err
}
