// Code generated by Thrift Compiler (0.15.0). DO NOT EDIT.

package rpc

import (
	"bytes"
	"context"
	"fmt"
	"time"
	thrift "github.com/apache/thrift/lib/go/thrift"
)

// (needed to ensure safety because of naive import list construction.)
var _ = thrift.ZERO
var _ = fmt.Printf
var _ = context.Background
var _ = time.Now
var _ = bytes.Equal

// Attributes:
//  - Name
//  - Bytes
type IEncoded struct {
  Name *string `thrift:"name,1" db:"name" json:"name,omitempty"`
  Bytes []byte `thrift:"bytes,2" db:"bytes" json:"bytes,omitempty"`
}

func NewIEncoded() *IEncoded {
  return &IEncoded{}
}

var IEncoded_Name_DEFAULT string
func (p *IEncoded) GetName() string {
  if !p.IsSetName() {
    return IEncoded_Name_DEFAULT
  }
return *p.Name
}
var IEncoded_Bytes_DEFAULT []byte

func (p *IEncoded) GetBytes() []byte {
  return p.Bytes
}
func (p *IEncoded) CountSetFieldsIEncoded() int {
  count := 0
  if (p.IsSetName()) {
    count++
  }
  if (p.IsSetBytes()) {
    count++
  }
  return count

}

func (p *IEncoded) IsSetName() bool {
  return p.Name != nil
}

func (p *IEncoded) IsSetBytes() bool {
  return p.Bytes != nil
}

func (p *IEncoded) Read(ctx context.Context, iprot thrift.TProtocol) error {
  if _, err := iprot.ReadStructBegin(ctx); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
  }


  for {
    _, fieldTypeId, fieldId, err := iprot.ReadFieldBegin(ctx)
    if err != nil {
      return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
    }
    if fieldTypeId == thrift.STOP { break; }
    switch fieldId {
    case 1:
      if fieldTypeId == thrift.STRING {
        if err := p.ReadField1(ctx, iprot); err != nil {
          return err
        }
      } else {
        if err := iprot.Skip(ctx, fieldTypeId); err != nil {
          return err
        }
      }
    case 2:
      if fieldTypeId == thrift.STRING {
        if err := p.ReadField2(ctx, iprot); err != nil {
          return err
        }
      } else {
        if err := iprot.Skip(ctx, fieldTypeId); err != nil {
          return err
        }
      }
    default:
      if err := iprot.Skip(ctx, fieldTypeId); err != nil {
        return err
      }
    }
    if err := iprot.ReadFieldEnd(ctx); err != nil {
      return err
    }
  }
  if err := iprot.ReadStructEnd(ctx); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
  }
  return nil
}

func (p *IEncoded)  ReadField1(ctx context.Context, iprot thrift.TProtocol) error {
  if v, err := iprot.ReadString(ctx); err != nil {
  return thrift.PrependError("error reading field 1: ", err)
} else {
  p.Name = &v
}
  return nil
}

func (p *IEncoded)  ReadField2(ctx context.Context, iprot thrift.TProtocol) error {
  if v, err := iprot.ReadBinary(ctx); err != nil {
  return thrift.PrependError("error reading field 2: ", err)
} else {
  p.Bytes = v
}
  return nil
}

func (p *IEncoded) Write(ctx context.Context, oprot thrift.TProtocol) error {
  if c := p.CountSetFieldsIEncoded(); c != 1 {
    return fmt.Errorf("%T write union: exactly one field must be set (%d set)", p, c)
  }
  if err := oprot.WriteStructBegin(ctx, "IEncoded"); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err) }
  if p != nil {
    if err := p.writeField1(ctx, oprot); err != nil { return err }
    if err := p.writeField2(ctx, oprot); err != nil { return err }
  }
  if err := oprot.WriteFieldStop(ctx); err != nil {
    return thrift.PrependError("write field stop error: ", err) }
  if err := oprot.WriteStructEnd(ctx); err != nil {
    return thrift.PrependError("write struct stop error: ", err) }
  return nil
}

func (p *IEncoded) writeField1(ctx context.Context, oprot thrift.TProtocol) (err error) {
  if p.IsSetName() {
    if err := oprot.WriteFieldBegin(ctx, "name", thrift.STRING, 1); err != nil {
      return thrift.PrependError(fmt.Sprintf("%T write field begin error 1:name: ", p), err) }
    if err := oprot.WriteString(ctx, string(*p.Name)); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T.name (1) field write error: ", p), err) }
    if err := oprot.WriteFieldEnd(ctx); err != nil {
      return thrift.PrependError(fmt.Sprintf("%T write field end error 1:name: ", p), err) }
  }
  return err
}

func (p *IEncoded) writeField2(ctx context.Context, oprot thrift.TProtocol) (err error) {
  if p.IsSetBytes() {
    if err := oprot.WriteFieldBegin(ctx, "bytes", thrift.STRING, 2); err != nil {
      return thrift.PrependError(fmt.Sprintf("%T write field begin error 2:bytes: ", p), err) }
    if err := oprot.WriteBinary(ctx, p.Bytes); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T.bytes (2) field write error: ", p), err) }
    if err := oprot.WriteFieldEnd(ctx); err != nil {
      return thrift.PrependError(fmt.Sprintf("%T write field end error 2:bytes: ", p), err) }
  }
  return err
}

func (p *IEncoded) Equals(other *IEncoded) bool {
  if p == other {
    return true
  } else if p == nil || other == nil {
    return false
  }
  if p.Name != other.Name {
    if p.Name == nil || other.Name == nil {
      return false
    }
    if (*p.Name) != (*other.Name) { return false }
  }
  if bytes.Compare(p.Bytes, other.Bytes) != 0 { return false }
  return true
}

func (p *IEncoded) String() string {
  if p == nil {
    return "<nil>"
  }
  return fmt.Sprintf("IEncoded(%+v)", *p)
}

// Attributes:
//  - Obj
//  - Params
type ISource struct {
  Obj *IEncoded `thrift:"obj,1,required" db:"obj" json:"obj"`
  Params map[string][]byte `thrift:"params,2" db:"params" json:"params"`
}

func NewISource() *ISource {
  return &ISource{
Params: map[string][]byte{
},
}
}

var ISource_Obj_DEFAULT *IEncoded
func (p *ISource) GetObj() *IEncoded {
  if !p.IsSetObj() {
    return ISource_Obj_DEFAULT
  }
return p.Obj
}

func (p *ISource) GetParams() map[string][]byte {
  return p.Params
}
func (p *ISource) IsSetObj() bool {
  return p.Obj != nil
}

func (p *ISource) Read(ctx context.Context, iprot thrift.TProtocol) error {
  if _, err := iprot.ReadStructBegin(ctx); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
  }

  var issetObj bool = false;

  for {
    _, fieldTypeId, fieldId, err := iprot.ReadFieldBegin(ctx)
    if err != nil {
      return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
    }
    if fieldTypeId == thrift.STOP { break; }
    switch fieldId {
    case 1:
      if fieldTypeId == thrift.STRUCT {
        if err := p.ReadField1(ctx, iprot); err != nil {
          return err
        }
        issetObj = true
      } else {
        if err := iprot.Skip(ctx, fieldTypeId); err != nil {
          return err
        }
      }
    case 2:
      if fieldTypeId == thrift.MAP {
        if err := p.ReadField2(ctx, iprot); err != nil {
          return err
        }
      } else {
        if err := iprot.Skip(ctx, fieldTypeId); err != nil {
          return err
        }
      }
    default:
      if err := iprot.Skip(ctx, fieldTypeId); err != nil {
        return err
      }
    }
    if err := iprot.ReadFieldEnd(ctx); err != nil {
      return err
    }
  }
  if err := iprot.ReadStructEnd(ctx); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
  }
  if !issetObj{
    return thrift.NewTProtocolExceptionWithType(thrift.INVALID_DATA, fmt.Errorf("Required field Obj is not set"));
  }
  return nil
}

func (p *ISource)  ReadField1(ctx context.Context, iprot thrift.TProtocol) error {
  p.Obj = &IEncoded{}
  if err := p.Obj.Read(ctx, iprot); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T error reading struct: ", p.Obj), err)
  }
  return nil
}

func (p *ISource)  ReadField2(ctx context.Context, iprot thrift.TProtocol) error {
  _, _, size, err := iprot.ReadMapBegin(ctx)
  if err != nil {
    return thrift.PrependError("error reading map begin: ", err)
  }
  tMap := make(map[string][]byte, size)
  p.Params =  tMap
  for i := 0; i < size; i ++ {
var _key0 string
    if v, err := iprot.ReadString(ctx); err != nil {
    return thrift.PrependError("error reading field 0: ", err)
} else {
    _key0 = v
}
var _val1 []byte
    if v, err := iprot.ReadBinary(ctx); err != nil {
    return thrift.PrependError("error reading field 0: ", err)
} else {
    _val1 = v
}
    p.Params[_key0] = _val1
  }
  if err := iprot.ReadMapEnd(ctx); err != nil {
    return thrift.PrependError("error reading map end: ", err)
  }
  return nil
}

func (p *ISource) Write(ctx context.Context, oprot thrift.TProtocol) error {
  if err := oprot.WriteStructBegin(ctx, "ISource"); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err) }
  if p != nil {
    if err := p.writeField1(ctx, oprot); err != nil { return err }
    if err := p.writeField2(ctx, oprot); err != nil { return err }
  }
  if err := oprot.WriteFieldStop(ctx); err != nil {
    return thrift.PrependError("write field stop error: ", err) }
  if err := oprot.WriteStructEnd(ctx); err != nil {
    return thrift.PrependError("write struct stop error: ", err) }
  return nil
}

func (p *ISource) writeField1(ctx context.Context, oprot thrift.TProtocol) (err error) {
  if err := oprot.WriteFieldBegin(ctx, "obj", thrift.STRUCT, 1); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write field begin error 1:obj: ", p), err) }
  if err := p.Obj.Write(ctx, oprot); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T error writing struct: ", p.Obj), err)
  }
  if err := oprot.WriteFieldEnd(ctx); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write field end error 1:obj: ", p), err) }
  return err
}

func (p *ISource) writeField2(ctx context.Context, oprot thrift.TProtocol) (err error) {
  if err := oprot.WriteFieldBegin(ctx, "params", thrift.MAP, 2); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write field begin error 2:params: ", p), err) }
  if err := oprot.WriteMapBegin(ctx, thrift.STRING, thrift.STRING, len(p.Params)); err != nil {
    return thrift.PrependError("error writing map begin: ", err)
  }
  for k, v := range p.Params {
    if err := oprot.WriteString(ctx, string(k)); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T. (0) field write error: ", p), err) }
    if err := oprot.WriteBinary(ctx, v); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T. (0) field write error: ", p), err) }
  }
  if err := oprot.WriteMapEnd(ctx); err != nil {
    return thrift.PrependError("error writing map end: ", err)
  }
  if err := oprot.WriteFieldEnd(ctx); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write field end error 2:params: ", p), err) }
  return err
}

func (p *ISource) Equals(other *ISource) bool {
  if p == other {
    return true
  } else if p == nil || other == nil {
    return false
  }
  if !p.Obj.Equals(other.Obj) { return false }
  if len(p.Params) != len(other.Params) { return false }
  for k, _tgt := range p.Params {
    _src2 := other.Params[k]
    if bytes.Compare(_tgt, _src2) != 0 { return false }
  }
  return true
}

func (p *ISource) String() string {
  if p == nil {
    return "<nil>"
  }
  return fmt.Sprintf("ISource(%+v)", *p)
}

