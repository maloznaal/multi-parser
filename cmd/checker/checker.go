package main

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"offline_parser/utils"
	"os"
	"time"
)

/*	Headers  = "record_type,record_id,start_timestamp,calling_party_number,called_party_number,redirecting_number,call_id_number,supplementary_services,cause,calling_party_category,call_duration,call_status,connected_number,imsi_calling,imei_calling,imsi_called,imei_called,msisdn_calling,msisdn_called,msc_number,vlr_number,location_lac,location_cell,
forwarding_reason,roaming_number,ss_code,ussd,operator_id,date_and_time,call_direction,seizure_time,answer_time,release_time"

2020/02/17 22:24:06 {-1 460128162 2019-12-02T18:00:29Z 77478844948 77081464716 - -1 - -1 -1 -1 -1 - - - 401770074763639 - - 77070200005 77070200005 -1 -1 -1 -1 - - - -1 2019-12-02T18:00:29Z -1 2019-12-02T18:00:29Z 2019-12-02T18:00:29Z 2019-12-02T18:00:29Z}
checker_1  | 2020/02/17 22:24:06 {-1 817455089 2019-12-02T18:00:36Z 77002447402 77089269197 - -1 - -1 -1 -1 -1 - - - 401770079245671 - - 77070200005 77070200005 -1 -1 -1 -1 - - - -1 2019-12-02T18:00:36Z -1 2019-12-02T18:00:36Z 2019-12-02T18:00:36Z 2019-12-02T18:00:36Z}
checker_1  | 2020/02/17 22:24:06 {-1 683024728 2019-12-02T18:00:41Z 77073686121 77075978013 - -1 - -1 -1 -1 -1 - - - 401770061616285 - - 77070100001 77070100001 -1 -1 -1 -1 - - - -1 2019-12-02T18:00:41Z -1 2019-12-02T18:00:41Z 2019-12-02T18:00:41Z 2019-12-02T18:00:41Z}
checker_1  | 2020/02/17 22:24:06 {-1 1006933274 2019-12-02T18:01:23Z 98768 77077751609 - -1 - -1 -1 -1 -1 - - - 401770052498397 - - 77070200001 77070200001 -1 -1 -1 -1 - - - -1 2019-12-02T18:01:23Z -1 2019-12-02T18:01:23Z 2019-12-02T18:01:23Z 2019-12-02T18:01:23Z}
checker_1  | 2020/02/17 22:24:06 {-1 607811211 2019-12-02T18:01:23Z Tele2 77075120413 - -1 - -1 -1 -1 -1 - - - 401770058556630 - - 8615654998 8615654998 -1 -1 -1 -1 - - - -1 2019-12-02T18:01:23Z -1 2019-12-02T18:01:23Z 2019-12-02T18:01:23Z 2019-12-02T18:01:23Z}
checker_1  | 2020/02/17 22:24:06 {-1 629431445 2019-12-02T18:00:30Z 77082947418 77089872351 - -1 - -1 -1 -1 -1 - - - 401770079739481 - - 77070200001 77070200001 -1 -1 -1 -1 - - - -1 2019-12-02T18:00:30Z -1 2019-12-02T18:00:30Z 2019-12-02T18:00:30Z 2019-12-02T18:00:30Z}
checker_1  | 2020/02/17 22:24:06 {-1 1458323237 2019-12-02T18:00:29Z 77082947418 77089872351 - -1 - -1 -1 -1 -1 - - - 401770070890689 - - 77070100001 77070100001 -1 -1 -1 -1 - - - -1 2019-12-02T18:00:29Z -1 2019-12-02T18:00:29Z 2019-12-02T18:00:29Z 2019-12-02T18:00:29Z}
checker_1  | 2020/02/17 22:24:06 {-1 469339106 2019-12-02T18:00:29Z 77082426840 77012362820 - -1 - -1 -1 -1 -1 - - - 401770079422920 - - 77070200001 77070200001 -1 -1 -1 -1 - - - -1 2019-12-02T18:00:29Z -1 2019-12-02T18:00:29Z 2019-12-02T18:00:29Z 2019-12-02T18:00:29Z}
checker_1  | 2020/02/17 22:24:06 {-1 436340495 2019-12-02T18:00:31Z 77082453457 77006850916 - -1 - -1 -1 -1 -1 - - - 401770078611997 - - 77070100001 77070100001 -1 -1 -1 -1 - - - -1 2019-12-02T18:00:31Z -1 2019-12-02T18:00:31Z 2019-12-02T18:00:31Z 2019-12-02T18:00:31Z}
checker_1  | 2020/02/17 22:24:06 {-1 774965466 2019-12-02T18:00:36Z 77715014467 77717446728 - -1 - -1 -1 -1 -1 - - - 401770079085595 - - 77070200001 77070200001 -1 -1 -1 -1 - - - -1 2019-12-02T18:00:36Z -1 2019-12-02T18:00:36Z 2019-12-02T18:00:36Z 2019-12-02T18:00:36Z}
checker_1  | 2020/02/17 22:24:06 {-1 1225511528 2019-12-02T18:00:30Z 77715014467 77717446728 - -1 - -1 -1 -1 -1 - - - 401770079085596 - - 77070200005 77070200005 -1 -1 -1 -1 - - - -1 2019-12-02T18:00:30Z -1 2019-12-02T18:00:30Z 2019-12-02T18:00:30Z 2019-12-02T18:00:30Z}
*/
const (
	insertedRowsNum = 11
)

func getRows(db *sql.DB) []utils.CDR {
	rows, err := db.Query("SELECT * FROM cdr_temp;")
	if err != nil {
		utils.HandleError(err, "Err query - select all")
	}

	defer rows.Close()
	cdrs := make([]utils.CDR, 0)
	cdr := utils.NewCdr()
	for rows.Next() {
		if err := rows.Scan(&cdr.RecordType, &cdr.RecordID, &cdr.StartTimestamp,
			&cdr.CallingPartyNumber,
			&cdr.CalledPartyNumber,
			&cdr.RedirectingNumber,
			&cdr.CallIDNumber,
			&cdr.SupplementaryServices,
			&cdr.Cause,
			&cdr.CallingPartyCategory,
			&cdr.CallDuration,
			&cdr.CallStatus,
			&cdr.ConnectedNumber,
			&cdr.ImsiCalling,
			&cdr.ImeiCalling,
			&cdr.ImsiCalled,
			&cdr.ImeiCalled,
			&cdr.MsisdnCalling,
			&cdr.MsisdnCalled,
			&cdr.MscNumber,
			&cdr.VlrNumber,
			&cdr.LocationLac,
			&cdr.LocationCell,
			&cdr.ForwardingReason,
			&cdr.RoamingNumber,
			&cdr.SsCode,
			&cdr.Ussd,
			&cdr.OperatorID,
			&cdr.DateAndTime,
			&cdr.CallDirection,
			&cdr.SeizureTime,
			&cdr.AnswerTime,
			&cdr.ReleaseTime); err != nil {
			panic(err)
		}
		cdrs = append(cdrs, cdr)
		log.Println(cdr)
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
	return cdrs
}

func connectDb() (*sql.DB, error) {
	conStr := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		"master", 5432, "gpadmin", "greenplum", "gpadmin")
	db, err := sql.Open("postgres", conStr)
	if err != nil {
		utils.HandleError(err, "Err openning db")
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		utils.HandleError(err, "No healty connection to db")
		return nil, err
	}
	return db, nil
}

func checkResult(cdrs []utils.CDR) {
	if len(cdrs) < insertedRowsNum {
		panic(errors.New(fmt.Sprintf("Rows not inserted, len(rows) - %d", len(cdrs))))
	}
	// gp in iTest with gmt tz +0
	// testCases
	cdr1 := utils.CDR{
		StartTimestamp:        "2019-12-02T18:00:29Z",
		CallingPartyNumber:    "77478844948",
		CalledPartyNumber:     "77081464716",
		ImsiCalled:            "401770074763639",
		MscNumber:             "77070200005",
		DateAndTime:           "2019-12-02T18:00:29Z",
		SeizureTime:           "2019-12-02T18:00:29Z",
		AnswerTime:            "2019-12-02T18:00:29Z",
		ReleaseTime:           "2019-12-02T18:00:29Z",
	}
	cdr2 := utils.CDR{
		StartTimestamp:        "2019-12-02T18:00:36Z",
		CallingPartyNumber:    "77002447402",
		CalledPartyNumber:     "77089269197",
		ImsiCalled:            "401770079245671",
		MscNumber:             "77070200005",
		DateAndTime:           "2019-12-02T18:00:36Z",
		SeizureTime:           "2019-12-02T18:00:36Z",
		AnswerTime:            "2019-12-02T18:00:36Z",
		ReleaseTime:           "2019-12-02T18:00:36Z",
	}
	cdr3 := utils.CDR{
		StartTimestamp:        "2019-12-02T18:00:41Z",
		CallingPartyNumber:    "77073686121",
		CalledPartyNumber:     "77075978013",
		ImsiCalled:            "401770061616285",
		MscNumber:             "77070100001",
		DateAndTime:           "2019-12-02T18:00:41Z",
		SeizureTime:           "2019-12-02T18:00:41Z",
		AnswerTime:            "2019-12-02T18:00:41Z",
		ReleaseTime:           "2019-12-02T18:00:41Z",
	}
	cdr4 := utils.CDR{
		StartTimestamp:        "2019-12-02T18:01:23Z",
		CallingPartyNumber:    "98768",
		CalledPartyNumber:     "77077751609",
		ImsiCalled:            "401770052498397",
		MscNumber:             "77070200001",
		DateAndTime:           "2019-12-02T18:01:23Z",
		SeizureTime:           "2019-12-02T18:01:23Z",
		AnswerTime:            "2019-12-02T18:01:23Z",
		ReleaseTime:           "2019-12-02T18:01:23Z",
	}
	cdr5 := utils.CDR{
		StartTimestamp:        "2019-12-02T18:01:23Z",
		CallingPartyNumber:    "Tele2",
		CalledPartyNumber:     "77075120413",
		ImsiCalled:            "401770058556630",
		MscNumber:             "8615654998",
		DateAndTime:           "2019-12-02T18:01:23Z",
		SeizureTime:           "2019-12-02T18:01:23Z",
		AnswerTime:            "2019-12-02T18:01:23Z",
		ReleaseTime:           "2019-12-02T18:01:23Z",
	}
	tc := make([]utils.CDR, 0)
	tc = append(tc, cdr1, cdr2, cdr3, cdr4, cdr5)
	for i, _ := range tc {
		if cdrs[i].StartTimestamp != tc[i].StartTimestamp {
			log.Fatalf("Err start_timestamp mismatch expected - %s, got $s at TEST %d", tc[i].StartTimestamp, cdrs[i].StartTimestamp, i)
		}
		if cdrs[i].CallingPartyNumber != tc[i].CallingPartyNumber {
			log.Fatalf("Err calling_party mismatch expected - %s, got %s at TEST %d" , cdrs[i].CallingPartyNumber, tc[i].CallingPartyNumber, i)
		}
		if cdrs[i].CalledPartyNumber != tc[i].CalledPartyNumber {
			log.Fatalf("Err called_party mismatch expected - %s, got %s at TEST %d", cdrs[i].CalledPartyNumber, tc[i].CalledPartyNumber, i)
		}
		if cdrs[i].ImsiCalled != tc[i].ImsiCalled {
			log.Fatalf("Err imsi_called mismatch, expected - %s, got %s at TEST %d", cdrs[i].ImsiCalled, tc[i].ImsiCalled, i)
		}
		if cdrs[i].MscNumber != tc[i].MscNumber {
			log.Fatalf("Err msc_num mismatch, expected - %s, got %s at TEST %d", cdrs[i].MscNumber, tc[i].MscNumber, i)
		}
		if cdrs[i].DateAndTime != tc[i].DateAndTime {
			log.Fatalf("Err date_and_time mismatch, expected - %s, got %s at TEST %d", cdrs[i].DateAndTime, tc[i].DateAndTime, i)
		}

		if cdrs[i].DateAndTime != tc[i].SeizureTime {
			log.Fatalf("Err seizure_time mismatch, expected - %s, got %s at TEST %d", cdrs[i].SeizureTime, tc[i].DateAndTime, i)
		}
		if cdrs[i].DateAndTime != tc[i].AnswerTime {
			log.Fatalf("Err answer_time mismatch, expected - %s, got %s at TEST %d", cdrs[i].AnswerTime, tc[i].DateAndTime, i)
		}
		if cdrs[i].DateAndTime != tc[i].ReleaseTime {
			log.Fatalf("Err release_time mismatch, expected - %s, got %s at TEST %d", cdrs[i].ReleaseTime, tc[i].DateAndTime, i)
		}
	}
}


func main() {
	stopChan := make(chan bool)
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
	// wait for init schema
	time.Sleep(20 * time.Second)
	cdrs := getRows(db)
	checkResult(cdrs)
	time.AfterFunc(5*time.Second, func() {
		log.Println("Integration TEST result with - SUCCESS")
		os.Exit(1)
	})
	<- stopChan
}
