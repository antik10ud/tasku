package uid16

import (
	"fmt"
	"sync"
	"errors"
	"time"
	"math/rand"
	"encoding/binary"
	"encoding/base64"
)

const (
	epochOffset = uint64(1520845232285679425)
	EncodeStd   = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~"
	max         = uint64(1<<64 - 1)
	size        = 16
	leftPad     = '0'
)

var (
	encodedSize = encoding.EncodedLen(size)
)

var encoding = base64.NewEncoding(EncodeStd).WithPadding(base64.NoPadding)

type Factory struct {
	clockOffset uint64
	lastTime    uint64
	lastSeq     uint8
	clockMutex  sync.Mutex
	timeFunc    func() uint64
	randFunc    func([]byte)
}

func NewFactory() *Factory {
	g := Factory{
		timeFunc: defaultTimeFunc,
		randFunc: defaultRandFunc,
	}
	return &g
}

type UId16 [size]byte

func (u UId16) Bytes() []byte {
	return u[:]
}

func (u UId16) String() string {
	return encoding.EncodeToString(u[:])
}

func (u *UId16) Shorten() string {
	input := u.String()
	i := 0
	for n := len(input) - 1; i < n && input[i] == leftPad; i++ {

	}
	return string([]byte(input)[i:])
}

func (gen *Factory) FromString(input string) (u UId16, err error) {
	v := []byte(input)
	k := encodedSize - len(v)
	if k < 0 {
		err = errors.New("uid16: invalid encoding")
		return
	} else if k > 0 {
		w := make([]byte, encodedSize)
		for i := 0; i < k; i++ {
			w[i] = leftPad
		}
		copy(w[k:], input)
		v = w
	}
	_, err = encoding.Decode(u[:], v)
	return
}

func (gen *Factory) FromBytes(input []byte) (u UId16, err error) {
	l := len(input)
	if l != size {
		err = fmt.Errorf("uid16: must be %d bytes max long", size)

	} else {
		copy(u[:], input)
	}
	return
}

func (gen *Factory) New() UId16 {
	timeNow := gen.timeFunc()
	gen.clockMutex.Lock()
	if timeNow < gen.lastTime {
		gen.clockOffset = (gen.lastTime - timeNow) + 1
	} else
	if timeNow == gen.lastTime {
		//chk it's not zero (!)
		gen.lastSeq++
	} else {
		gen.lastSeq = 0
	}
	gen.lastTime = timeNow
	seq := gen.lastSeq
	timeNow = timeNow + gen.clockOffset
	gen.clockMutex.Unlock()
	u := UId16{}
	binary.BigEndian.PutUint64(u[0:], timeNow)
	u[8] = seq
	gen.randFunc(u[9:])
	return u
}

func maxId16() UId16 {
	u := UId16{}
	for i := 0; i < len(u); i++ {
		u[i] = 255
	}
	return u
}

func minId16() UId16 {
	u := UId16{}
	for i := 0; i < len(u); i++ {
		u[i] = 0
	}
	return u
}

func defaultRandFunc(slice []byte) {

	rand.Read(slice)
}

func defaultTimeFunc() uint64 {
	return uint64(time.Now().UnixNano()) - epochOffset
}
