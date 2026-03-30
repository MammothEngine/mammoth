package bson

// BSONType represents a BSON value type.
type BSONType byte

const (
	TypeDouble      BSONType = 0x01
	TypeString      BSONType = 0x02
	TypeDocument    BSONType = 0x03
	TypeArray       BSONType = 0x04
	TypeBinary      BSONType = 0x05
	TypeUndefined   BSONType = 0x06
	TypeObjectID    BSONType = 0x07
	TypeBoolean     BSONType = 0x08
	TypeDateTime    BSONType = 0x09
	TypeNull        BSONType = 0x0A
	TypeRegex       BSONType = 0x0B
	TypeDBPointer   BSONType = 0x0C
	TypeJavaScript  BSONType = 0x0D
	TypeSymbol      BSONType = 0x0E
	TypeCodeScope   BSONType = 0x0F
	TypeInt32       BSONType = 0x10
	TypeTimestamp   BSONType = 0x11
	TypeInt64       BSONType = 0x12
	TypeDecimal128  BSONType = 0x13
	TypeMinKey      BSONType = 0xFF
	TypeMaxKey      BSONType = 0x7F
)

// BinarySubtype represents the subtype of a BSON Binary value.
type BinarySubtype byte

const (
	BinaryGeneric    BinarySubtype = 0x00
	BinaryFunction   BinarySubtype = 0x01
	BinaryOld        BinarySubtype = 0x02
	BinaryUUIDOld    BinarySubtype = 0x03
	BinaryUUID       BinarySubtype = 0x04
	BinaryMD5        BinarySubtype = 0x05
	BinaryEncrypted  BinarySubtype = 0x06
	BinaryColumn     BinarySubtype = 0x07
	BinaryUser       BinarySubtype = 0x80
)
