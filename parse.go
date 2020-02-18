package main

import (
	"compress/flate"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"offline_parser/utils"
	"os"
	"path/filepath"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	"github.com/mholt/archiver/v3"
)


var zipNames []string

var m = map[string]bool{}
var c Config
var test_mode = 0

type Config struct {
	dbProps struct {
		PG_USERNAME string
		PG_PASSWORD string
		PG_DBNAME string
		PG_PORT int
		PG_HOST string
	}
}

// const paths
const (
	tmpDirName   = "/app/tmp"
	tmpDirPath   = "/app/tmp/"
	cleanZipPath = "/app/czips/"
	dirtyZipPath = "/app/zips/"
	gpfdistAddr  = "gpfdist:8888"
)

const (
	PG_USERNAME = "gpadmin"
	PG_PASSWORD ="greenplum"
	PG_DBNAME = "gpadmin"
	PG_PORT = 5432
	PG_HOST = "master"
)

func init() {
	utils.Gen(len(utils.S), 3, "", &m)
	if os.Getenv("TEST") != "" {
		test_mode, _ = strconv.Atoi(os.Getenv("TEST"))
	}
}


func connectDb() (*sql.DB, error) {
	conStr := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		PG_HOST, PG_PORT, PG_USERNAME, PG_PASSWORD, PG_DBNAME)
	db, err := sql.Open("postgres", conStr)
	if err != nil {
		utils.HandleError(err, "Err openning db")
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		utils.HandleError(err, "No healthy connection to db")
		return nil, err
	}
	return db, nil
}

// init schema for iTest
func initSchema(db *sql.DB) {
	tx, err := db.Begin()
	if err != nil {
		utils.HandleError(err, "Err couldn't init schema" )
		panic(err)
	}
	dropQuery := "DROP TABLE IF EXISTS public.cdr_temp; "
	createQuery := "CREATE TABLE public.cdr_temp ( " +
		"record_type int2, " +
		"record_id int4, " +
		"start_timestamp timestamptz(6) NOT NULL, " +
		"calling_party_number varchar(34), " +
		"called_party_number varchar(34) NOT NULL, " +
		"redirecting_number varchar(34), " +
		"call_id_number int8, " +
		"supplementary_services text, " +
		"cause int4, " +
		"calling_party_category int2, " +
		"call_duration int4, " +
		"call_status int2, " +
		"connected_number varchar(34), " +
		"imsi_calling varchar(16), " +
		"imei_calling varchar(16), " +
		"imsi_called varchar(16), " +
		"imei_called varchar(16), " +
		"msisdn_calling varchar(34), " +
		"msisdn_called varchar(34), " +
		"msc_number varchar(18), " +
		"vlr_number varchar(34), " +
		"location_lac int4, " +
		"location_cell int4, " +
		"forwarding_reason int2, " +
		"roaming_number varchar(34), " +
		"ss_code text, " +
		"ussd text, " +
		"operator_id int8, " +
		"date_and_time timestamptz(6), " +
		"call_direction int2, " +
		"seizureTime timestamptz(6), " +
	    "answerTime timestamptz(6), " +
		"releaseTime timestamptz(6)" +
		") WITH (appendonly=true, compresstype=zlib, compresslevel=4, orientation=column) ";

	// first query
	{
		stmt, err := tx.Prepare(dropQuery)
		if err != nil {
			utils.HandleError(err, "Fail on preparing drop query")
			tx.Rollback()
		}
		defer stmt.Close()
		if _, err := stmt.Exec(); err != nil {
			utils.HandleError(err, "Fail on executing drop query")
			tx.Rollback()
		}
	}
	// second query
	{
		stmt, err := tx.Prepare(createQuery)
		if err != nil {
			utils.HandleError(err, "Fail on preparing create query")
			tx.Rollback()
		}
		defer stmt.Close()
		if _, err := stmt.Exec(); err != nil {
			utils.HandleError(err, "Fail on executing create query")
			tx.Rollback()
		}
	}

	tx.Commit()
}

func insertBatch(db *sql.DB, batch int) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	// first query
	{
		q := fmt.Sprintf(`CREATE EXTERNAL TABLE ext_batch%d ( LIKE cdr_temp )`, batch) +
			fmt.Sprintf(`LOCATION ('gpfdist://%s/%s')`, gpfdistAddr, "*.gz") +
			`FORMAT 'CSV' (HEADER FORCE NOT NULL start_timestamp)` +
			`LOG ERRORS SEGMENT REJECT LIMIT 10 ROWS;`
		stmt, err := tx.Prepare(q)
		if err != nil {
			utils.HandleError(err, fmt.Sprintf("Err tx on prepare with batchnum %d", batch))
			tx.Rollback()
			return err
		}
		defer stmt.Close()
		if _, err := stmt.Exec(); err != nil {
			utils.HandleError(err, fmt.Sprintf("Err tx on exec with batchnum %d", batch))
			tx.Rollback()
			return err
		}
	}
	// second query
	{
		stmt, err := tx.Prepare(fmt.Sprintf(`INSERT INTO cdr_temp SELECT * FROM ext_batch%d;`, batch))
		if err != nil {
			log.Println(fmt.Sprintf("Err preparing batch query for batchnum %d", batch), err)
			tx.Rollback()
			return err
		}
		defer stmt.Close()
		if _, err := stmt.Exec(); err != nil {
			log.Println(fmt.Sprintf("Err executing batch query for batchnum %d ", batch), err)
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func main() {

	// wait until gp is up
	db, err := connectDb()
	for i := 0; i < 600; i++ {
		log.Println("Trying to connect to db...")
		db, err = connectDb()
		if err == nil && db != nil {
			defer db.Close()
			break
		}
		time.Sleep(5*time.Second)
	}

	// initialize schema for iTest if flag persist
	if test_mode == 1 {
		log.Println("Init schema cdr_temp for iTest")
		initSchema(db)
	}

	stopChan := make(chan bool)
	z := archiver.NewTarGz()
	z.CompressionLevel = flate.DefaultCompression
	z.SingleThreaded = false
	err = z.Create(os.Stdout)


	if err != nil {
		utils.HandleError(err, fmt.Sprintf("Err creating archiver with stdout writer"))
	}
	defer z.Close()
	files, err := ioutil.ReadDir(dirtyZipPath)
	if err != nil {
		utils.HandleError(err, fmt.Sprintf("Err reading dir %s check permission granted", dirtyZipPath))
		panic(err)
	}
	for _, f := range files {
		if !f.IsDir() {
			zipNames = append(zipNames, f.Name())
		}
	}

	for _, zname := range zipNames {

		// walkFn for each zip
		err := z.Walk(dirtyZipPath+zname, func(f archiver.File) error {
			if f.IsDir() {
				return nil
			}
			valz := utils.ReadCsv(f, zname)
			cdrs := utils.ParseJob(valz, &m)
			utils.WriteJob(f.Name(), tmpDirName, cdrs)
			return nil
		})

		// corrupted zip
		if err != nil {
			utils.HandleError(err, fmt.Sprintf("Corrupted zip with name %s, skipping...", zname))
			if ok := utils.RemoveContents(tmpDirPath); ok != nil {
				utils.HandleError(ok, fmt.Sprintf("Error removing contents at path %s", tmpDirPath))
			}
			continue // skip zip
		}

		files, err := filepath.Glob(fmt.Sprintf("%s*", tmpDirPath))

		if err != nil {
			utils.HandleError(err, fmt.Sprintf("Err while reading contents of %s dir", tmpDirPath))
		}

		// produce cleanZip
		z.Archive(files, fmt.Sprintf("%s%s", cleanZipPath, zname))

		// flush /tmp dir
		if ok := utils.RemoveContents(tmpDirPath); ok != nil {
			utils.HandleError(ok, fmt.Sprintf("Error removing contents at path %s", tmpDirPath))
		}

		s := rand.NewSource(time.Now().UnixNano())
		r := rand.New(s)
		batch := r.Intn((1 << 31) - 1)
		// insertCleanZip into db
		if ok := insertBatch(db, batch); ok != nil {
			utils.HandleError(err, fmt.Sprintf("Err transaction with batch num %d", batch) )
		}

		// flush cleanZip
		if ok := utils.RemoveContents(cleanZipPath); ok != nil {
			utils.HandleError(ok, fmt.Sprintf("Error removing contents at path %s", cleanZipPath))
		}

		log.Printf("Done loading zip %s with batch num %d", zname, batch)
	}

	// blocking
	<-stopChan
}
