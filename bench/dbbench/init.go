package main

var bitalosDB *BitalosDB
var atomickeyIndex int32
var randValueList [][]byte

func initDB() {
	atomickeyIndex = 0

	var err error
	if *dbType == DB_BITALOS {
		dbDir := "./" + *dbName + "/bitalosdb"
		bitalosDB, err = openBitalosDB(dbDir, "")
		if err != nil {
			panic(any(err))
		}
	}

	randValueList = make([][]byte, randValueCount)
	for i := 0; i < randValueCount; i++ {
		randVal := make([]byte, *vlen)
		randValueList[i] = randValue(randVal, *vlen)
	}
}
