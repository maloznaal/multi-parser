package utils

import (
	"strconv"
	"time"
)

// CDR record
type CDR struct {
	RecordType            int
	RecordID              int32
	StartTimestamp        time.Time
	CallingPartyNumber    string
	CalledPartyNumber     string
	RedirectingNumber     string
	CallIDNumber          int
	SupplementaryServices string
	Cause                 int
	CallingPartyCategory  int
	CallDuration          int
	CallStatus            int
	ConnectedNumber       string
	ImsiCalling           string
	ImeiCalling           string
	ImsiCalled            string
	ImeiCalled            string
	MsisdnCalling         string
	MsisdnCalled          string
	MscNumber             string
	VlrNumber             int
	LocationLac           int
	LocationCell          int
	ForwardingReason      int
	RoamingNumber         string
	SsCode                string
	Ussd                  string
	OperatorID            int
	DateAndTime           time.Time
	CallDirection         int
	SeizureTime           time.Time
	AnswerTime            time.Time
	ReleaseTime           time.Time
	DateToWrite string
}

// NewCdr create new instance of Cdr with default fields

func NewCdr() CDR {
	c := CDR{}
	c.RecordType = -1
	c.RecordID = -1
	c.StartTimestamp = time.Time{}
	c.CallingPartyNumber = "-"
	c.CalledPartyNumber = "-"
	c.RedirectingNumber = "-"
	c.CallIDNumber = -1
	c.SupplementaryServices = "-"
	c.Cause = -1
	c.CallingPartyCategory = -1
	c.CallDuration = -1
	c.CallStatus = -1
	c.ConnectedNumber = "-"
	c.ImsiCalling = "-"
	c.ImeiCalling = "-"
	c.ImsiCalled = "-"
	c.ImeiCalled = "-"
	c.MsisdnCalling = "-"
	c.MsisdnCalled = "-"
	c.MscNumber = "-"
	c.VlrNumber = -1
	c.LocationLac = -1
	c.LocationCell = -1
	c.ForwardingReason = -1
	c.RoamingNumber = "-"
	c.SsCode = "-"
	c.Ussd = "-"
	c.OperatorID = -1
	c.DateAndTime = time.Time{}
	c.CallDirection = -1
	c.SeizureTime = time.Time{}
	c.AnswerTime = time.Time{}
	c.ReleaseTime = time.Time{}
	c.DateToWrite = "-"
	return c
}

// return slice of strings[] -> for csv write
func (c CDR) csvRow() []string {
	cdrs := make([]string, 0)
	cdrs = append(cdrs, strconv.Itoa(c.RecordType))

	cdrs = append(cdrs, fastConvert(c.RecordID))
	cdrs = append(cdrs, c.DateToWrite)
	cdrs = append(cdrs, c.CallingPartyNumber)
	cdrs = append(cdrs, c.CalledPartyNumber)
	cdrs = append(cdrs, c.RedirectingNumber)
	cdrs = append(cdrs, strconv.Itoa(c.CallIDNumber))
	cdrs = append(cdrs, c.SupplementaryServices)
	cdrs = append(cdrs, strconv.Itoa(c.Cause))

	cdrs = append(cdrs, strconv.Itoa(c.CallingPartyCategory))
	cdrs = append(cdrs, strconv.Itoa(c.CallDuration))
	cdrs = append(cdrs, strconv.Itoa(c.CallStatus))
	cdrs = append(cdrs, c.ConnectedNumber)
	cdrs = append(cdrs, c.ImsiCalling)
	cdrs = append(cdrs, c.ImeiCalling)
	cdrs = append(cdrs, c.ImsiCalled)
	cdrs = append(cdrs, c.ImeiCalled, c.MsisdnCalling, c.MsisdnCalled, c.MscNumber)
	cdrs = append(cdrs, strconv.Itoa(c.VlrNumber), strconv.Itoa(c.LocationLac), strconv.Itoa(c.LocationCell), strconv.Itoa(c.ForwardingReason))
	cdrs = append(cdrs, c.RoamingNumber, c.SsCode, c.Ussd)
	cdrs = append(cdrs, strconv.Itoa(c.OperatorID))
	cdrs = append(cdrs, c.DateToWrite)
	cdrs = append(cdrs, strconv.Itoa(c.CallDirection))
	cdrs = append(cdrs, c.DateToWrite, c.DateToWrite, c.DateToWrite) // time.Time

	return cdrs
}

// convert w/o null bytes
func fastConvert(n int32) string {
	b := [11]byte{}
	a := len(b)
	i := int64(n)
	signed := i < 0
	if signed {
		i = -i
	}
	for {
		a--
		b[a], i = '0'+byte(i%10), i/10
		if i == 0 {
			if signed {
				a--
				b[a] = '-'
			}
			return string(b[a:])
		}
	}
}
