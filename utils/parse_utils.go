package utils

import (
	"encoding/csv"
	"fmt"
	"github.com/mholt/archiver/v3"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	Headers  = "record_type,record_id,start_timestamp,calling_party_number,called_party_number,redirecting_number,call_id_number,supplementary_services,cause,calling_party_category,call_duration,call_status,connected_number,imsi_calling,imei_calling,imsi_called,imei_called,msisdn_calling,msisdn_called,msc_number,vlr_number,location_lac,location_cell,forwarding_reason,roaming_number,ss_code,ussd,operator_id,date_and_time,call_direction,seizure_time,answer_time,release_time"
	S  = "ABCDE0123456789" // alphabet
)

// position of headers 0-based
const (
	SmsIDpos = 0
	num1Pos = 3
	num2Pos = 4
	dirPos = 5
	datePos = 7
	statusPos = 8
	imsiPos = 12
	mscPos = 13
)

var r = []rune(S)

// HandleError handles error
func HandleError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %s", msg, err)
	}
}

// TimeTrack returns time per function
func TimeTrack(start time.Time, name string) {
	dur := time.Since(start)
	log.Printf("%s took %s", name, dur)
}

// ReadCsv - reads csv file into valz
func ReadCsv(f archiver.File, zipName string) [][]string {
	valz := make([][]string, 0)
	csvr := csv.NewReader(f)
	for {
		row, err := csvr.Read()
		if err != nil {
			if err == csv.ErrFieldCount {
				HandleError(err, fmt.Sprintf("err parsing row in zip %s file %s", zipName, f.Name()))
				err = nil
				valz = append(valz, row) // still can treat as valid entry for us
			}
			if err == io.EOF {
				return valz
			}
		}
		valz = append(valz, row)
	}
}

// WriteJob writes headers & cdr content to file
func WriteJob(file string, dir string, cdrs []CDR) {
	recordFile, err := os.Create(fmt.Sprintf("%s/%s", dir, file))
	if err != nil {
		HandleError(err, fmt.Sprintf("Err creating csv with name %s", file))
		return
	}

	writer := csv.NewWriter(recordFile)
	// writing headers
	err = writer.Write(strings.Split(Headers, ","))
	if err != nil {
		HandleError(err, fmt.Sprintf("Err writing headers to file with name %s", file))
	}
	for _, cdr := range cdrs {
		data := cdr.csvRow()

		err = writer.Write(data)
		if err != nil {
			HandleError(err, fmt.Sprintf("Err writing data to file %s", file))
			return // skip
		}

		writer.Flush()
		err = writer.Error()
		if err != nil {
			HandleError(err, fmt.Sprintf("Err writing to file %s", file))
			return
		}
	}

	err = recordFile.Close()
	if err != nil {
		HandleError(err, fmt.Sprintf("Err closing file %s", file))
		return
	}
}

// ParseJob parses slice of strings in place -> ready for writing
func ParseJob(valz [][]string, m *map[string]bool) []CDR{
	recID := ""
	cdrs := make([]CDR, 0)
	for _, chunks := range valz {
		c, r := chunks[num1Pos], chunks[num2Pos]
		suf := c[3:]
		if len(c) > 11 && AreDigits(suf) && (*m)[c[:3]] {
			chunks[3] = suf
		}

		suf = r[3:]
		if len(r) > 11 && AreDigits(suf) && (*m)[r[:3]] {
			chunks[4] = suf
		}

		cdr := NewCdr()
		recID = chunks[SmsIDpos]
		i, err := strconv.ParseInt(recID, 10, 64)
		if err != nil {
			HandleError(err, fmt.Sprintf("Err parsing record id with val %d", i))
		}

		cdr.RecordID = int32(i & ((1<<32)-1))
		dir, err := strconv.Atoi(chunks[dirPos])
		if err != nil {
			HandleError(err, "err converting dir")
			continue // skip record
		}
		if dir == 1 {
			cdr.CallingPartyNumber = chunks[num1Pos]
			cdr.CalledPartyNumber = chunks[num2Pos]
		} else {
			cdr.CallingPartyNumber = chunks[num2Pos]
			cdr.CalledPartyNumber = chunks[num1Pos]
		}

		// short number is abonent probably
		if len(cdr.CalledPartyNumber) < 9 || !AreDigits(cdr.CalledPartyNumber) {
			cdr.RecordType = 18001
		} else {
			cdr.RecordType = 18000 // mt
		}


		ts, err := PostgresTime(chunks[datePos])
		if err != nil {
			HandleError(err, "err parsing time")
			continue // skip record
		}
		cdr.StartTimestamp = ts
		cdr.DateAndTime = ts

		cdr.MscNumber = chunks[mscPos]
		cdr.ImsiCalled = chunks[imsiPos]

		cdr.SeizureTime = ts
		cdr.AnswerTime = ts
		cdr.ReleaseTime = ts
		//cdr.MsisdnCalled = chunks[mscPos]
		cdr.MscNumber = chunks[mscPos]
		cdrs = append(cdrs, cdr)
	}
	return cdrs
}

// PostgresTime - returns time in PostgresFormat, no such timestamp provided in go Time package.
func PostgresTime(date string) (string, error)  {
	if len(date) < 14 {
		return "", fmt.Errorf("err parsing timestamp with entry len - %d", len(date))
	}
	yyyy := date[0:4]
	mm := date[4:6]
	dd  := date[6:8]
	hh := date[8:10]
	min := date[10:12]
	ss := date[12:]
	rez := fmt.Sprintf("%s-%s-%s %s:%s:%s+06", yyyy, mm, dd, hh, min, ss)
	return rez, nil
}

// RemoveContents - removes content of a dir
func RemoveContents(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}

// AreDigits - O(n) ASCI check of each rune 2 times faster than regexp.MatchString("^[0-9]*$", suf)
func AreDigits(suf string) bool {
	for _, c := range suf {
		if c < 48 || c > 57 {
			return false
		}
	}
	return true
}

// Gen  - generates all permuts of size k=3 with alphabet size - n, excluding all only digit subsets from result set
func Gen(n int, k int, res string, m *map[string]bool) {
	if k == 0 {
		if !AreDigits(res) {
			(*m)[res] = true
		}
		return
	}
	for i := 0; i < n; i++ {
		prefix := res + string(r[i])
		Gen(n, k-1, prefix, m)
	}
}
