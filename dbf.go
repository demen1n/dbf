package dbf

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
)

type FileType byte

func (ft FileType) String() string {
	switch ft {
	case FoxBASE:
		return "FoxBASE"
	case FoxBASEPlusNoMemo:
		return "FoxBASE+/Dbase III plus, no memo"
	case VisualFoxPro:
		return "Visual FoxPro"
	case VisualFoxProAI:
		return "Visual FoxPro, autoincrement enabled"
	case VisualFoxProVarchar:
		return "Visual FoxPro with field type Varchar or Varbinary"
	case dBASEIVTF:
		return "dBASE IV SQL table files, no memo"
	case dBASEIVSF:
		return "dBASE IV SQL system files, no memo"
	case FoxBASEPlusMemo:
		return "FoxBASE+/dBASE III PLUS, with memo"
	case dBASEIVMemo:
		return "dBASE IV with memo"
	case dBASEIVTFMemo:
		return "dBASE IV SQL table files with memo"
	case FoxPro2:
		return "FoxPro 2.x (or earlier) with memo"
	case HiPerSix:
		return "HiPer-Six format with SMT memo file"
	default:
		return fmt.Sprintf("Unknown (0x%02X)", byte(ft))
	}
}

const (
	FoxBASE             FileType = 0x02
	FoxBASEPlusNoMemo   FileType = 0x03
	VisualFoxPro        FileType = 0x30
	VisualFoxProAI      FileType = 0x31
	VisualFoxProVarchar FileType = 0x32
	dBASEIVTF           FileType = 0x43
	dBASEIVSF           FileType = 0x63
	FoxBASEPlusMemo     FileType = 0x83
	dBASEIVMemo         FileType = 0x8B
	dBASEIVTFMemo       FileType = 0xCB
	FoxPro2             FileType = 0xF5
	HiPerSix            FileType = 0xE5
)

const (
	metadataLength uint16 = 32
	fieldLength    uint16 = 32
)

type Field struct {
	Name          string
	Type          byte
	MemoryAddress uint32
	Length        byte
	DecimalCount  byte
}

func (f Field) TypeString() string {
	switch f.Type {
	case 'C':
		return "Character"
	case 'N':
		return "Numeric"
	case 'D':
		return "Date"
	case 'L':
		return "Logical"
	case 'M':
		return "Memo"
	case 'F':
		return "Float"
	default:
		return fmt.Sprintf("Unknown (%c)", f.Type)
	}
}

type Reader struct {
	fileType          FileType
	lastUpdate        time.Time
	recordsCount      uint32
	headerBytesNumber uint16
	recordBytesNumber uint16
	fieldsCount       uint16
	fields            []Field

	decoder       *encoding.Decoder
	reader        *bufio.Reader
	currentRecord uint32 // Текущая позиция для Next()
	err           error  // Последняя ошибка при чтении
}

type Option func(*Reader)

func WithDecoder(decoder *encoding.Decoder) Option {
	return func(r *Reader) {
		r.decoder = decoder
	}
}

func WithEncoding(cm *charmap.Charmap) Option {
	return WithDecoder(cm.NewDecoder())
}

func WithCP866() Option {
	return WithEncoding(charmap.CodePage866)
}

func WithCP1251() Option {
	return WithEncoding(charmap.Windows1251)
}

func WithCP1252() Option {
	return WithEncoding(charmap.Windows1252)
}

func New(r io.Reader, opts ...Option) (*Reader, error) {
	reader := &Reader{
		reader: bufio.NewReader(r),
	}

	for _, opt := range opts {
		opt(reader)
	}

	if err := reader.readMetadata(); err != nil {
		return nil, fmt.Errorf("read metadata: %w", err)
	}

	if reader.decoder == nil {
		return nil, fmt.Errorf("unable to determine encoding: please specify encoding explicitly using WithCP866(), WithCP1251() or WithEncoding()")
	}

	if err := reader.readFields(); err != nil {
		return nil, fmt.Errorf("read fields: %w", err)
	}

	return reader, nil
}

func NewFromFile(path string, opts ...Option) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	return New(file, opts...)
}

func (r *Reader) FileType() FileType {
	return r.fileType
}

func (r *Reader) LastUpdate() time.Time {
	return r.lastUpdate
}

func (r *Reader) RecordsCount() uint32 {
	return r.recordsCount
}

func (r *Reader) Fields() []Field {
	return r.fields
}

func (r *Reader) FieldsCount() int {
	return len(r.fields)
}

func (r *Reader) readMetadata() error {
	// file type
	b, err := r.reader.ReadByte()
	if err != nil {
		return fmt.Errorf("read file type: %w", err)
	}

	fileType := FileType(b)
	if !isValidFileType(fileType) {
		return fmt.Errorf("unknown file type: 0x%02X", b)
	}
	r.fileType = fileType

	// date last update (3 byte: YY MM DD)
	dateBytes := make([]byte, 3)
	if _, err := io.ReadFull(r.reader, dateBytes); err != nil {
		return fmt.Errorf("read last update date: %w", err)
	}

	r.lastUpdate = time.Date(
		int(dateBytes[0])+1900,
		time.Month(dateBytes[1]),
		int(dateBytes[2]),
		0, 0, 0, 0,
		time.UTC,
	)

	// record count (4 byte, little-endian)
	recordsBytes := make([]byte, 4)
	if _, err := io.ReadFull(r.reader, recordsBytes); err != nil {
		return fmt.Errorf("read records count: %w", err)
	}
	r.recordsCount = binary.LittleEndian.Uint32(recordsBytes)

	// header size (2 byte, little-endian)
	headerBytes := make([]byte, 2)
	if _, err := io.ReadFull(r.reader, headerBytes); err != nil {
		return fmt.Errorf("read header size: %w", err)
	}
	r.headerBytesNumber = binary.LittleEndian.Uint16(headerBytes)
	r.fieldsCount = (r.headerBytesNumber - metadataLength) / fieldLength

	// record size (2 bye, little-endian)
	recordBytes := make([]byte, 2)
	if _, err := io.ReadFull(r.reader, recordBytes); err != nil {
		return fmt.Errorf("read record size: %w", err)
	}
	r.recordBytesNumber = binary.LittleEndian.Uint16(recordBytes)

	// reserved byte
	reserved := make([]byte, 20)
	if _, err := io.ReadFull(r.reader, reserved); err != nil {
		return fmt.Errorf("read reserved bytes: %w", err)
	}

	// byte 29 (index 17) - Language Driver ID
	// try to set decoder by LDID
	if r.decoder == nil {
		languageDriverID := reserved[17]
		r.decoder = getDecoderByLDID(languageDriverID)
	}

	return nil
}

func (r *Reader) readFields() error {
	r.fields = make([]Field, 0, r.fieldsCount)

	for i := uint16(0); i < r.fieldsCount; i++ {
		field, err := r.readField()
		if err != nil {
			return fmt.Errorf("read field %d: %w", i, err)
		}
		r.fields = append(r.fields, field)
	}

	// end of file (0x0D)
	terminator, err := r.reader.ReadByte()
	if err != nil {
		return fmt.Errorf("read terminator: %w", err)
	}
	if terminator != 0x0D {
		return fmt.Errorf("invalid field descriptor terminator: 0x%02X, expected 0x0D", terminator)
	}

	return nil
}

func (r *Reader) readField() (Field, error) {
	fieldBytes := make([]byte, 32)
	if _, err := io.ReadFull(r.reader, fieldBytes); err != nil {
		return Field{}, fmt.Errorf("read field bytes: %w", err)
	}

	// filed name (11 byte)
	nameBytes := bytes.TrimRight(fieldBytes[0:11], "\x00")
	decodedName, err := r.decoder.Bytes(nameBytes)
	if err != nil {
		// если не получается декодировать, используем как есть
		decodedName = nameBytes
	}

	field := Field{
		Name:          string(decodedName),
		Type:          fieldBytes[11],
		MemoryAddress: binary.LittleEndian.Uint32(fieldBytes[12:16]),
		Length:        fieldBytes[16],
		DecimalCount:  fieldBytes[17],
	}

	return field, nil
}

func isValidFileType(ft FileType) bool {
	switch ft {
	case FoxBASE, FoxBASEPlusNoMemo, VisualFoxPro, VisualFoxProAI,
		VisualFoxProVarchar, dBASEIVTF, dBASEIVSF, FoxBASEPlusMemo,
		dBASEIVMemo, dBASEIVTFMemo, FoxPro2, HiPerSix:
		return true
	default:
		return false
	}
}

func getDecoderByLDID(ldid byte) *encoding.Decoder {
	switch ldid {
	case 0x26: // CP866 (Russian MS-DOS)
		return charmap.CodePage866.NewDecoder()
	case 0x64, 0x65, 0xC9: // CP1251 (Russian Windows)
		return charmap.Windows1251.NewDecoder()
	case 0x03: // CP1252 (Windows ANSI)
		return charmap.Windows1252.NewDecoder()
	case 0x01: // CP437 (US MS-DOS)
		return charmap.CodePage437.NewDecoder()
	case 0x02: // CP850 (International MS-DOS)
		return charmap.CodePage850.NewDecoder()
	default:
		return nil
	}
}

func (r *Reader) String() string {
	return fmt.Sprintf(
		"DBF Reader:\n"+
			"  File Type: %s\n"+
			"  Last Update: %s\n"+
			"  Records Count: %d\n"+
			"  Fields Count: %d\n"+
			"  Header Size: %d bytes\n"+
			"  Record Size: %d bytes",
		r.fileType,
		r.lastUpdate.Format("2006-01-02"),
		r.recordsCount,
		r.fieldsCount,
		r.headerBytesNumber,
		r.recordBytesNumber,
	)
}

type Record struct {
	Deleted bool              // флаг удаления (первый байт записи)
	Data    map[string]string // данные полей (имя -> значение)
}

func (r *Reader) Next() bool {
	if r.currentRecord >= r.recordsCount {
		return false
	}

	r.currentRecord++
	return r.err == nil
}

func (r *Reader) Read() (*Record, error) {
	if r.err != nil {
		return nil, r.err
	}

	// read all record
	recordBytes := make([]byte, r.recordBytesNumber)
	if _, err := io.ReadFull(r.reader, recordBytes); err != nil {
		r.err = fmt.Errorf("read record bytes: %w", err)
		return nil, r.err
	}

	record := &Record{
		Deleted: recordBytes[0] == 0x2A, // '*' for deleted records
		Data:    make(map[string]string, len(r.fields)),
	}

	// fields
	offset := 1 // deletion flag
	for _, field := range r.fields {
		fieldData := recordBytes[offset : offset+int(field.Length)]
		offset += int(field.Length)

		// field value
		value, err := r.decodeFieldValue(field, fieldData)
		if err != nil {
			r.err = fmt.Errorf("decode field %s: %w", field.Name, err)
			return nil, r.err
		}

		record.Data[field.Name] = value
	}

	return record, nil
}

func (r *Reader) ReadAll() ([]*Record, error) {
	records := make([]*Record, 0, r.recordsCount)

	for r.Next() {
		record, err := r.Read()
		if err != nil {
			return records, err
		}
		records = append(records, record)
	}

	if err := r.Err(); err != nil {
		return records, err
	}

	return records, nil
}

func (r *Reader) Err() error {
	if r.err == io.EOF {
		return nil
	}
	return r.err
}

func (r *Reader) decodeFieldValue(field Field, data []byte) (string, error) {
	trimmed := bytes.TrimSpace(data)

	switch field.Type {
	case 'C': // character
		decoded, err := r.decoder.Bytes(trimmed)
		if err != nil {
			return string(trimmed), nil // fallback на оригинал
		}
		return string(decoded), nil

	case 'N', 'F': // numeric, Float
		return string(trimmed), nil

	case 'D': // date - format YYYYMMDD
		return string(trimmed), nil

	case 'L': // logical - boolean
		if len(trimmed) > 0 {
			switch trimmed[0] {
			case 'T', 't', 'Y', 'y':
				return "true", nil
			case 'F', 'f', 'N', 'n':
				return "false", nil
			}
		}
		return "", nil

	case 'M': // memo
		return string(trimmed), nil

	default:
		// unknown type - try to parse like string
		decoded, err := r.decoder.Bytes(trimmed)
		if err != nil {
			return string(trimmed), nil
		}
		return string(decoded), nil
	}
}
