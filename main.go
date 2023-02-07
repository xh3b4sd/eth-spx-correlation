package main

import (
	"bytes"
	"encoding/csv"
	"log"
	"sort"
	"strconv"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/xh3b4sd/eth-spx-correlation/pkg/apicliaws"
	"github.com/xh3b4sd/eth-spx-correlation/pkg/slicer"
	"github.com/xh3b4sd/framer"
)

const (
	bucnam = "chiron-data-collector"
	filpat = "eth-spx-correlation/coefficient.csv"
	pateth = "eth/prices.csv"
	patspx = "spx/spx.csv"
)

type csvrow struct {
	Tim time.Time
	Wee float64
	Mon float64
}

func main() {
	var err error

	var cli *apicliaws.AWS
	{
		cli = apicliaws.New()
	}

	var byteth []byte
	{
		byteth, err = cli.Download(bucnam, pateth)
		if apicliaws.IsNotFound(err) {
			// fall through
		} else if err != nil {
			panic(err)
		}
	}

	var roweth [][]string
	{
		roweth, err = csv.NewReader(bytes.NewReader(byteth)).ReadAll()
		if err != nil {
			log.Fatal(err)
		}
	}

	staeth := time.Time{}
	endeth := time.Time{}
	cureth := map[time.Time]float64{}
	for _, x := range roweth[1:] {
		var t time.Time
		{
			t = mustim(x[0])
		}

		if staeth.IsZero() || t.Before(staeth) {
			staeth = t
		}

		if endeth.IsZero() || t.After(endeth) {
			endeth = t
		}

		{
			cureth[t] = musf64(x[1])
		}
	}

	var bytspx []byte
	{
		bytspx, err = cli.Download(bucnam, patspx)
		if apicliaws.IsNotFound(err) {
			// fall through
		} else if err != nil {
			panic(err)
		}
	}

	var rowspx [][]string
	{
		rowspx, err = csv.NewReader(bytes.NewReader(bytspx)).ReadAll()
		if err != nil {
			log.Fatal(err)
		}
	}

	staspx := time.Time{}
	endspx := time.Time{}
	curspx := map[time.Time]float64{}
	for _, x := range rowspx[1:] {
		var t time.Time
		{
			t = mustim(x[0])
		}

		if staspx.IsZero() || t.Before(staspx) {
			staspx = t
		}

		if endspx.IsZero() || t.After(endspx) {
			endspx = t
		}

		{
			curspx[t] = musf64(x[1])
		}
	}

	var sta time.Time
	if staeth.Before(staspx) {
		sta = staeth
	} else {
		sta = staspx
	}

	var end time.Time
	if endeth.After(endspx) {
		end = endeth
	} else {
		end = endspx
	}

	var fra *framer.Framer
	{
		fra = framer.New(framer.Config{
			Sta: sta,
			End: end,
			Len: 24 * time.Hour,
		})
	}

	var weeeth *slicer.Slicer
	var weespx *slicer.Slicer
	var moneth *slicer.Slicer
	var monspx *slicer.Slicer
	{
		weeeth = &slicer.Slicer{His: 7}
		weespx = &slicer.Slicer{His: 7}
		moneth = &slicer.Slicer{His: 30}
		monspx = &slicer.Slicer{His: 30}
	}

	var lis []csvrow
	for _, x := range fra.List() {
		{
			weeeth.Add(cureth[x.Sta])
			weespx.Add(curspx[x.Sta])
			moneth.Add(cureth[x.Sta])
			monspx.Add(curspx[x.Sta])
		}

		var w float64
		if weeeth.Red() && weespx.Red() {
			w, err = stats.Correlation(weeeth.Lis, weespx.Lis)
			if err != nil {
				log.Fatal(err)
			}
		}

		var m float64
		if moneth.Red() && monspx.Red() {
			m, err = stats.Correlation(moneth.Lis, monspx.Lis)
			if err != nil {
				log.Fatal(err)
			}
		}

		if w != 0 && m != 0 {
			lis = append(lis, csvrow{Tim: x.Sta, Wee: w, Mon: m})
		}
	}

	{
		sort.SliceStable(lis, func(i, j int) bool { return lis[i].Tim.Before(lis[j].Tim) })
	}

	var res [][]string
	{
		res = append(res, []string{"date", "weekly", "monthly"})
	}

	var wri *bytes.Buffer
	{
		wri = bytes.NewBufferString("")
	}

	{
		err = csv.NewWriter(wri).WriteAll(res)
		if err != nil {
			log.Fatal(err)
		}
	}

	{
		err = cli.Upload(bucnam, filpat, *bytes.NewReader(wri.Bytes()))
		if err != nil {
			panic(err)
		}
	}
}

func musf64(str string) float64 {
	f64, err := strconv.ParseFloat(str, 64)
	if err != nil {
		log.Fatal(err)
	}

	return f64
}

func mustim(str string) time.Time {
	tim, err := time.Parse(time.RFC3339, str)
	if err != nil {
		log.Fatal(err)
	}

	return tim
}
