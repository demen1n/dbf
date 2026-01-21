package dbf

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"golang.org/x/text/encoding/charmap"
)

// createMinimalDBF creates a minimal valid DBF file for testing
func createMinimalDBF() []byte {
	buf := new(bytes.Buffer)

	// header (32 bytes)
	buf.WriteByte(0x03) // file type: FoxBASE+/Dbase III plus, no memo
	buf.WriteByte(124)  // last update: year (124 = 2024)
	buf.WriteByte(1)    // month
	buf.WriteByte(15)   // day

	// number of records (4 bytes, little-endian)
	binary.Write(buf, binary.LittleEndian, uint32(2))

	// header size (2 bytes) - 32 (header) + 32 (field) + 1 (terminator)
	binary.Write(buf, binary.LittleEndian, uint16(32+32+1))

	// record size (2 bytes) - 1 (deletion flag) + 10 (NAME field)
	binary.Write(buf, binary.LittleEndian, uint16(11))

	// reserved (20 bytes)
	buf.Write(make([]byte, 20))

	// field descriptor (32 bytes)
	name := []byte("NAME")
	name = append(name, make([]byte, 11-len(name))...) // pad to 11 bytes
	buf.Write(name)

	buf.WriteByte('C')          // field type: Character
	buf.Write(make([]byte, 4))  // memory address (reserved)
	buf.WriteByte(10)           // field length
	buf.WriteByte(0)            // decimal count
	buf.Write(make([]byte, 14)) // reserved

	// field descriptor terminator
	buf.WriteByte(0x0D)

	// records
	// record 1 (not deleted)
	buf.WriteByte(0x20)           // not deleted
	buf.WriteString("John Doe  ") // 10 bytes

	// record 2 (deleted)
	buf.WriteByte(0x2A)           // deleted (*)
	buf.WriteString("Jane Smith") // 10 bytes

	return buf.Bytes()
}

// createDBFWithMultipleFields creates a DBF with multiple field types
func createDBFWithMultipleFields() []byte {
	buf := new(bytes.Buffer)

	// header
	buf.WriteByte(0x03)
	buf.WriteByte(124) // 2024
	buf.WriteByte(6)   // june
	buf.WriteByte(15)

	binary.Write(buf, binary.LittleEndian, uint32(1))         // 1 record
	binary.Write(buf, binary.LittleEndian, uint16(32+32*3+1)) // 3 fields
	binary.Write(buf, binary.LittleEndian, uint16(1+10+3+8))  // record size

	// reserved bytes (20 total) - add LDID for autodetect
	reserved := make([]byte, 20)
	reserved[17] = 0x26 // language Driver ID = CP866
	buf.Write(reserved)

	// field 1: NAME (Character)
	name := append([]byte("NAME"), make([]byte, 7)...)
	buf.Write(name)
	buf.WriteByte('C')
	buf.Write(make([]byte, 4))
	buf.WriteByte(10)
	buf.WriteByte(0)
	buf.Write(make([]byte, 14))

	// field 2: AGE (Numeric)
	age := append([]byte("AGE"), make([]byte, 8)...)
	buf.Write(age)
	buf.WriteByte('N')
	buf.Write(make([]byte, 4))
	buf.WriteByte(3)
	buf.WriteByte(0)
	buf.Write(make([]byte, 14))

	// field 3: BIRTHDATE (Date)
	birthdate := append([]byte("BIRTHDATE"), make([]byte, 2)...)
	buf.Write(birthdate)
	buf.WriteByte('D')
	buf.Write(make([]byte, 4))
	buf.WriteByte(8)
	buf.WriteByte(0)
	buf.Write(make([]byte, 14))

	buf.WriteByte(0x0D)

	// record
	buf.WriteByte(0x20)
	buf.WriteString("Alice     ") // 10 bytes
	buf.WriteString(" 25")        // 3 bytes
	buf.WriteString("19990115")   // 8 bytes

	return buf.Bytes()
}

func TestNew(t *testing.T) {
	data := createMinimalDBF()
	reader := bytes.NewReader(data)

	dbf, err := New(reader, WithCP866())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	if dbf == nil {
		t.Fatal("New() returned nil")
	}

	if dbf.RecordsCount() != 2 {
		t.Errorf("Expected 2 records, got %d", dbf.RecordsCount())
	}

	if dbf.FieldsCount() != 1 {
		t.Errorf("Expected 1 field, got %d", dbf.FieldsCount())
	}
}

func TestNewWithOptions(t *testing.T) {
	data := createMinimalDBF()
	reader := bytes.NewReader(data)

	// test with CP866
	dbf, err := New(reader, WithCP866())
	if err != nil {
		t.Fatalf("New() with WithCP866() failed: %v", err)
	}
	if dbf == nil {
		t.Fatal("New() returned nil")
	}

	// test with custom encoding
	reader2 := bytes.NewReader(data)
	dbf2, err := New(reader2, WithEncoding(charmap.Windows1251))
	if err != nil {
		t.Fatalf("New() with WithEncoding() failed: %v", err)
	}
	if dbf2 == nil {
		t.Fatal("New() returned nil")
	}

	// test with custom decoder
	reader3 := bytes.NewReader(data)
	decoder := charmap.CodePage850.NewDecoder()
	dbf3, err := New(reader3, WithDecoder(decoder))
	if err != nil {
		t.Fatalf("New() with WithDecoder() failed: %v", err)
	}
	if dbf3 == nil {
		t.Fatal("New() returned nil")
	}
}

func TestMultipleOptions(t *testing.T) {
	data := createMinimalDBF()
	reader := bytes.NewReader(data)

	// last option should win
	dbf, err := New(reader, WithCP866(), WithCP1251())
	if err != nil {
		t.Fatalf("New() with multiple options failed: %v", err)
	}
	if dbf == nil {
		t.Fatal("New() returned nil")
	}
}

func TestFileType(t *testing.T) {
	data := createMinimalDBF()
	reader := bytes.NewReader(data)

	dbf, err := New(reader, WithCP866())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	if dbf.FileType() != FoxBASEPlusNoMemo {
		t.Errorf("Expected FileType FoxBASEPlusNoMemo (0x03), got %v", dbf.FileType())
	}

	if dbf.FileType().String() == "" {
		t.Error("FileType.String() returned empty string")
	}
}

func TestLastUpdate(t *testing.T) {
	data := createMinimalDBF()
	reader := bytes.NewReader(data)

	dbf, err := New(reader, WithCP866())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	expected := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	if !dbf.LastUpdate().Equal(expected) {
		t.Errorf("Expected LastUpdate %v, got %v", expected, dbf.LastUpdate())
	}
}

func TestFields(t *testing.T) {
	data := createDBFWithMultipleFields()
	reader := bytes.NewReader(data)

	dbf, err := New(reader, WithCP866())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	fields := dbf.Fields()
	if len(fields) != 3 {
		t.Fatalf("Expected 3 fields, got %d", len(fields))
	}

	// check NAME field
	if fields[0].Name != "NAME" {
		t.Errorf("Expected field name 'NAME', got '%s'", fields[0].Name)
	}
	if fields[0].Type != 'C' {
		t.Errorf("Expected field type 'C', got '%c'", fields[0].Type)
	}
	if fields[0].Length != 10 {
		t.Errorf("Expected field length 10, got %d", fields[0].Length)
	}

	// check AGE field
	if fields[1].Name != "AGE" {
		t.Errorf("Expected field name 'AGE', got '%s'", fields[1].Name)
	}
	if fields[1].Type != 'N' {
		t.Errorf("Expected field type 'N', got '%c'", fields[1].Type)
	}

	// check BIRTHDATE field
	if fields[2].Name != "BIRTHDATE" {
		t.Errorf("Expected field name 'BIRTHDATE', got '%s'", fields[2].Name)
	}
	if fields[2].Type != 'D' {
		t.Errorf("Expected field type 'D', got '%c'", fields[2].Type)
	}
}

func TestFieldTypeString(t *testing.T) {
	tests := []struct {
		fieldType byte
		expected  string
	}{
		{'C', "Character"},
		{'N', "Numeric"},
		{'D', "Date"},
		{'L', "Logical"},
		{'M', "Memo"},
		{'F', "Float"},
		{'X', "Unknown (X)"},
	}

	for _, tt := range tests {
		field := Field{Type: tt.fieldType}
		if got := field.TypeString(); got != tt.expected {
			t.Errorf("Field type %c: expected '%s', got '%s'", tt.fieldType, tt.expected, got)
		}
	}
}

func TestReadAll(t *testing.T) {
	data := createMinimalDBF()
	reader := bytes.NewReader(data)

	dbf, err := New(reader, WithCP866())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	records, err := dbf.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() failed: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("Expected 2 records, got %d", len(records))
	}

	// check first record (not deleted)
	if records[0].Deleted {
		t.Error("First record should not be deleted")
	}

	name := strings.TrimSpace(records[0].Data["NAME"])
	if name != "John Doe" {
		t.Errorf("Expected name 'John Doe', got '%s'", name)
	}

	// check second record (deleted)
	if !records[1].Deleted {
		t.Error("Second record should be deleted")
	}
}

func TestNextAndRead(t *testing.T) {
	data := createMinimalDBF()
	reader := bytes.NewReader(data)

	dbf, err := New(reader, WithCP866())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	count := 0
	for dbf.Next() {
		record, err := dbf.Read()
		if err != nil {
			t.Fatalf("Read() failed: %v", err)
		}

		if record == nil {
			t.Fatal("Read() returned nil record")
		}

		count++
	}

	if err := dbf.Err(); err != nil {
		t.Fatalf("Err() returned error: %v", err)
	}

	if count != 2 {
		t.Errorf("Expected to read 2 records, got %d", count)
	}
}

func TestMultipleFieldTypes(t *testing.T) {
	data := createDBFWithMultipleFields()
	reader := bytes.NewReader(data)

	dbf, err := New(reader, WithCP866())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	records, err := dbf.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() failed: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(records))
	}

	record := records[0]

	// check NAME
	name := strings.TrimSpace(record.Data["NAME"])
	if name != "Alice" {
		t.Errorf("Expected name 'Alice', got '%s'", name)
	}

	// check AGE
	age := strings.TrimSpace(record.Data["AGE"])
	if age != "25" {
		t.Errorf("Expected age '25', got '%s'", age)
	}

	// check BIRTHDATE
	birthdate := strings.TrimSpace(record.Data["BIRTHDATE"])
	if birthdate != "19990115" {
		t.Errorf("Expected birthdate '19990115', got '%s'", birthdate)
	}
}

func TestInvalidFileType(t *testing.T) {
	buf := new(bytes.Buffer)
	buf.WriteByte(0xFF) // invalid file type
	buf.Write(make([]byte, 100))

	_, err := New(bytes.NewReader(buf.Bytes()), WithCP866())
	if err == nil {
		t.Error("Expected error for invalid file type, got nil")
	}
}

func TestEncodingAutodetection(t *testing.T) {
	buf := new(bytes.Buffer)

	// valid header
	buf.WriteByte(0x03)
	buf.WriteByte(124) // 2024
	buf.WriteByte(1)   // january
	buf.WriteByte(15)

	binary.Write(buf, binary.LittleEndian, uint32(0))       // 0 records
	binary.Write(buf, binary.LittleEndian, uint16(32+32+1)) // 1 field
	binary.Write(buf, binary.LittleEndian, uint16(11))      // record size

	// reserved bytes (20 total)
	reserved := make([]byte, 20)
	reserved[17] = 0x26 // language Driver ID = CP866
	buf.Write(reserved)

	// field descriptor (нужен хотя бы один!)
	name := append([]byte("NAME"), make([]byte, 7)...)
	buf.Write(name)
	buf.WriteByte('C')
	buf.Write(make([]byte, 4))
	buf.WriteByte(10)
	buf.WriteByte(0)
	buf.Write(make([]byte, 14))

	// terminator
	buf.WriteByte(0x0D)

	// should auto-detect CP866 from LDID
	reader := bytes.NewReader(buf.Bytes())
	dbf, err := New(reader) // без явной кодировки!
	if err != nil {
		t.Fatalf("New() with autodetection failed: %v", err)
	}

	if dbf == nil {
		t.Fatal("New() returned nil")
	}

	// verify it detected the encoding correctly
	if dbf.FieldsCount() != 1 {
		t.Errorf("Expected 1 field, got %d", dbf.FieldsCount())
	}
}

func TestEncodingAutodetectionFails(t *testing.T) {
	buf := new(bytes.Buffer)

	// valid header
	buf.WriteByte(0x03)
	buf.WriteByte(124)
	buf.WriteByte(1)
	buf.WriteByte(15)

	binary.Write(buf, binary.LittleEndian, uint32(0))
	binary.Write(buf, binary.LittleEndian, uint16(32+32+1))
	binary.Write(buf, binary.LittleEndian, uint16(11))

	// reserved bytes with unknown LDID
	reserved := make([]byte, 20)
	reserved[17] = 0xFF // unknown Language Driver ID
	buf.Write(reserved)

	// field descriptor
	name := append([]byte("NAME"), make([]byte, 7)...)
	buf.Write(name)
	buf.WriteByte('C')
	buf.Write(make([]byte, 4))
	buf.WriteByte(10)
	buf.WriteByte(0)
	buf.Write(make([]byte, 14))

	buf.WriteByte(0x0D)

	// should fail - no explicit encoding and can't auto-detect
	reader := bytes.NewReader(buf.Bytes())
	_, err := New(reader)
	if err == nil {
		t.Error("Expected error when encoding can't be determined, got nil")
	}

	// error message should be helpful
	if !strings.Contains(err.Error(), "specify encoding explicitly") {
		t.Errorf("Error message should mention explicit encoding, got: %v", err)
	}
}

func TestInvalidTerminator(t *testing.T) {
	buf := new(bytes.Buffer)

	// valid header
	buf.WriteByte(0x03)
	buf.Write(make([]byte, 10))
	binary.Write(buf, binary.LittleEndian, uint32(0)) // 0 records
	binary.Write(buf, binary.LittleEndian, uint16(32+32+1))
	binary.Write(buf, binary.LittleEndian, uint16(11))
	buf.Write(make([]byte, 20))

	// field descriptor
	buf.Write(make([]byte, 32))

	// invalid terminator (should be 0x0D)
	buf.WriteByte(0xFF)

	_, err := New(bytes.NewReader(buf.Bytes()))
	if err == nil {
		t.Error("Expected error for invalid terminator, got nil")
	}
}

func TestEmptyFile(t *testing.T) {
	_, err := New(bytes.NewReader([]byte{}))
	if err == nil {
		t.Error("Expected error for empty file, got nil")
	}
}

func TestTruncatedFile(t *testing.T) {
	data := createMinimalDBF()
	truncated := data[:10] // only first 10 bytes

	_, err := New(bytes.NewReader(truncated))
	if err == nil {
		t.Error("Expected error for truncated file, got nil")
	}
}

func TestString(t *testing.T) {
	data := createMinimalDBF()
	reader := bytes.NewReader(data)

	dbf, err := New(reader, WithCP866())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	str := dbf.String()
	if str == "" {
		t.Error("String() returned empty string")
	}

	// check that string contains expected information
	if !strings.Contains(str, "Records Count: 2") {
		t.Error("String() doesn't contain record count")
	}
}

func TestEOFBehavior(t *testing.T) {
	data := createMinimalDBF()
	reader := bytes.NewReader(data)

	dbf, err := New(reader, WithCP866())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// read all records
	count := 0
	for dbf.Next() {
		_, err := dbf.Read()
		if err != nil {
			t.Fatalf("Read() failed: %v", err)
		}
		count++
	}

	// try to read past EOF
	if dbf.Next() {
		t.Error("Next() should return false after EOF")
	}

	// err() should not return io.EOF
	if err := dbf.Err(); err != nil {
		t.Errorf("Err() should return nil after normal EOF, got: %v", err)
	}
}

func TestZeroRecords(t *testing.T) {
	buf := new(bytes.Buffer)

	buf.WriteByte(0x03)
	buf.WriteByte(124)
	buf.WriteByte(1)
	buf.WriteByte(15)

	binary.Write(buf, binary.LittleEndian, uint32(0)) // 0 records
	binary.Write(buf, binary.LittleEndian, uint16(32+32+1))
	binary.Write(buf, binary.LittleEndian, uint16(11))
	buf.Write(make([]byte, 20))

	// field descriptor
	name := append([]byte("NAME"), make([]byte, 7)...)
	buf.Write(name)
	buf.WriteByte('C')
	buf.Write(make([]byte, 4))
	buf.WriteByte(10)
	buf.WriteByte(0)
	buf.Write(make([]byte, 14))
	buf.WriteByte(0x0D)

	reader := bytes.NewReader(buf.Bytes())
	dbf, err := New(reader, WithCP866())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	if dbf.RecordsCount() != 0 {
		t.Errorf("Expected 0 records, got %d", dbf.RecordsCount())
	}

	records, err := dbf.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() failed: %v", err)
	}

	if len(records) != 0 {
		t.Errorf("Expected 0 records from ReadAll(), got %d", len(records))
	}
}

// Benchmark tests
func BenchmarkNew(b *testing.B) {
	data := createMinimalDBF()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(data)
		_, err := New(reader, WithCP866())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadAll(b *testing.B) {
	data := createMinimalDBF()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(data)
		dbf, _ := New(reader, WithCP866())
		_, err := dbf.ReadAll()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNextRead(b *testing.B) {
	data := createMinimalDBF()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(data)
		dbf, _ := New(reader, WithCP866())
		for dbf.Next() {
			_, err := dbf.Read()
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}
