package species

import (
	"fmt"

	"github.com/pkg/errors"
)

type DataFile interface {
	// Open(filepath string) error
	GetRows(sheet string) ([][]string, error)
	GetCols(sheet string) ([][]string, error)
	GetSheet() []string
	GetTable(sheet string) (Table, error)
	GetFirstRow(sheet string) ([]string, int, error)
}

func stringArrArrToInterfaceArrArr(strArrArr [][]string) [][]interface{} {
	irr := [][]interface{}{}
	for _, strArr := range strArrArr {
		irr = append(irr, stringArrToInterfaceArr(strArr))
	}
	return irr
}

func stringArrToInterfaceArr(strArr []string) []interface{} {
	iArr := []interface{}{}
	for _, str := range strArr {
		iArr = append(iArr, str)
	}
	return iArr
}

func CreateAllSheetTable(df DataFile, db *DB) error {
	for _, sheet := range df.GetSheet() {
		fmt.Println("开始处理 sheet" + sheet)
		err := CreateSheetTable(df, sheet, db)
		if err != nil {
			return errors.WithMessage(err, "CreateSheetTable")
		}
	}
	return nil
}
func CreateSheetTable(df DataFile, sheet string, db *DB) error {

	tab, err := df.GetTable(sheet)
	fmt.Println("CreateSheetTable：table", tab)
	if err != nil {
		return errors.WithMessage(err, "df.GetTable")
	}
	err = db.CreateTable(tab)
	if err != nil {
		return errors.WithMessage(err, "db.createTable")
	}
	return nil
}
