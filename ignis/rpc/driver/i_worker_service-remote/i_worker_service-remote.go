// Code generated by Thrift Compiler (0.15.0). DO NOT EDIT.

package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	thrift "github.com/apache/thrift/lib/go/thrift"
	"ignis/rpc"
	"ignis/rpc/driver"
)

var _ = rpc.GoUnusedProtection__
var _ = driver.GoUnusedProtection__

func Usage() {
  fmt.Fprintln(os.Stderr, "Usage of ", os.Args[0], " [-h host:port] [-u url] [-f[ramed]] function [arg1 [arg2...]]:")
  flag.PrintDefaults()
  fmt.Fprintln(os.Stderr, "\nFunctions:")
  fmt.Fprintln(os.Stderr, "  void start(IWorkerId id)")
  fmt.Fprintln(os.Stderr, "  void destroy(IWorkerId id)")
  fmt.Fprintln(os.Stderr, "  IWorkerId newInstance(i64 id, string type)")
  fmt.Fprintln(os.Stderr, "  IWorkerId newInstance3(i64 id, string name, string type)")
  fmt.Fprintln(os.Stderr, "  IWorkerId newInstance4(i64 id, string type, i32 cores, i32 instances)")
  fmt.Fprintln(os.Stderr, "  IWorkerId newInstance5(i64 id, string name, string type, i32 cores, i32 instances)")
  fmt.Fprintln(os.Stderr, "  void setName(IWorkerId id, string name)")
  fmt.Fprintln(os.Stderr, "  IDataFrameId parallelize(IWorkerId id, i64 dataId, i64 partitions)")
  fmt.Fprintln(os.Stderr, "  IDataFrameId parallelize4(IWorkerId id, i64 dataId, i64 partitions, ISource src)")
  fmt.Fprintln(os.Stderr, "  IDataFrameId importDataFrame(IWorkerId id, IDataFrameId data)")
  fmt.Fprintln(os.Stderr, "  IDataFrameId importDataFrame3(IWorkerId id, IDataFrameId data, ISource src)")
  fmt.Fprintln(os.Stderr, "  IDataFrameId plainFile(IWorkerId id, string path, i8 delim)")
  fmt.Fprintln(os.Stderr, "  IDataFrameId plainFile4(IWorkerId id, string path, i64 minPartitions, i8 delim)")
  fmt.Fprintln(os.Stderr, "  IDataFrameId textFile(IWorkerId id, string path)")
  fmt.Fprintln(os.Stderr, "  IDataFrameId textFile3(IWorkerId id, string path, i64 minPartitions)")
  fmt.Fprintln(os.Stderr, "  IDataFrameId partitionObjectFile(IWorkerId id, string path)")
  fmt.Fprintln(os.Stderr, "  IDataFrameId partitionObjectFile3(IWorkerId id, string path, ISource src)")
  fmt.Fprintln(os.Stderr, "  IDataFrameId partitionTextFile(IWorkerId id, string path)")
  fmt.Fprintln(os.Stderr, "  IDataFrameId partitionJsonFile3a(IWorkerId id, string path, bool objectMapping)")
  fmt.Fprintln(os.Stderr, "  IDataFrameId partitionJsonFile3b(IWorkerId id, string path, ISource src)")
  fmt.Fprintln(os.Stderr, "  void loadLibrary(IWorkerId id, string lib)")
  fmt.Fprintln(os.Stderr, "  void execute(IWorkerId id, ISource src)")
  fmt.Fprintln(os.Stderr, "  IDataFrameId executeTo(IWorkerId id, ISource src)")
  fmt.Fprintln(os.Stderr, "  void voidCall(IWorkerId id, ISource src)")
  fmt.Fprintln(os.Stderr, "  void voidCall3(IWorkerId id, IDataFrameId data, ISource src)")
  fmt.Fprintln(os.Stderr, "  IDataFrameId call(IWorkerId id, ISource src)")
  fmt.Fprintln(os.Stderr, "  IDataFrameId call3(IWorkerId id, IDataFrameId data, ISource src)")
  fmt.Fprintln(os.Stderr)
  os.Exit(0)
}

type httpHeaders map[string]string

func (h httpHeaders) String() string {
  var m map[string]string = h
  return fmt.Sprintf("%s", m)
}

func (h httpHeaders) Set(value string) error {
  parts := strings.Split(value, ": ")
  if len(parts) != 2 {
    return fmt.Errorf("header should be of format 'Key: Value'")
  }
  h[parts[0]] = parts[1]
  return nil
}

func main() {
  flag.Usage = Usage
  var host string
  var port int
  var protocol string
  var urlString string
  var framed bool
  var useHttp bool
  headers := make(httpHeaders)
  var parsedUrl *url.URL
  var trans thrift.TTransport
  _ = strconv.Atoi
  _ = math.Abs
  flag.Usage = Usage
  flag.StringVar(&host, "h", "localhost", "Specify host and port")
  flag.IntVar(&port, "p", 9090, "Specify port")
  flag.StringVar(&protocol, "P", "binary", "Specify the protocol (binary, compact, simplejson, json)")
  flag.StringVar(&urlString, "u", "", "Specify the url")
  flag.BoolVar(&framed, "framed", false, "Use framed transport")
  flag.BoolVar(&useHttp, "http", false, "Use http")
  flag.Var(headers, "H", "Headers to set on the http(s) request (e.g. -H \"Key: Value\")")
  flag.Parse()
  
  if len(urlString) > 0 {
    var err error
    parsedUrl, err = url.Parse(urlString)
    if err != nil {
      fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
      flag.Usage()
    }
    host = parsedUrl.Host
    useHttp = len(parsedUrl.Scheme) <= 0 || parsedUrl.Scheme == "http" || parsedUrl.Scheme == "https"
  } else if useHttp {
    _, err := url.Parse(fmt.Sprint("http://", host, ":", port))
    if err != nil {
      fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
      flag.Usage()
    }
  }
  
  cmd := flag.Arg(0)
  var err error
  var cfg *thrift.TConfiguration = nil
  if useHttp {
    trans, err = thrift.NewTHttpClient(parsedUrl.String())
    if len(headers) > 0 {
      httptrans := trans.(*thrift.THttpClient)
      for key, value := range headers {
        httptrans.SetHeader(key, value)
      }
    }
  } else {
    portStr := fmt.Sprint(port)
    if strings.Contains(host, ":") {
           host, portStr, err = net.SplitHostPort(host)
           if err != nil {
                   fmt.Fprintln(os.Stderr, "error with host:", err)
                   os.Exit(1)
           }
    }
    trans = thrift.NewTSocketConf(net.JoinHostPort(host, portStr), cfg)
    if err != nil {
      fmt.Fprintln(os.Stderr, "error resolving address:", err)
      os.Exit(1)
    }
    if framed {
      trans = thrift.NewTFramedTransportConf(trans, cfg)
    }
  }
  if err != nil {
    fmt.Fprintln(os.Stderr, "Error creating transport", err)
    os.Exit(1)
  }
  defer trans.Close()
  var protocolFactory thrift.TProtocolFactory
  switch protocol {
  case "compact":
    protocolFactory = thrift.NewTCompactProtocolFactoryConf(cfg)
    break
  case "simplejson":
    protocolFactory = thrift.NewTSimpleJSONProtocolFactoryConf(cfg)
    break
  case "json":
    protocolFactory = thrift.NewTJSONProtocolFactory()
    break
  case "binary", "":
    protocolFactory = thrift.NewTBinaryProtocolFactoryConf(cfg)
    break
  default:
    fmt.Fprintln(os.Stderr, "Invalid protocol specified: ", protocol)
    Usage()
    os.Exit(1)
  }
  iprot := protocolFactory.GetProtocol(trans)
  oprot := protocolFactory.GetProtocol(trans)
  client := driver.NewIWorkerServiceClient(thrift.NewTStandardClient(iprot, oprot))
  if err := trans.Open(); err != nil {
    fmt.Fprintln(os.Stderr, "Error opening socket to ", host, ":", port, " ", err)
    os.Exit(1)
  }
  
  switch cmd {
  case "start":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Start requires 1 args")
      flag.Usage()
    }
    arg103 := flag.Arg(1)
    mbTrans104 := thrift.NewTMemoryBufferLen(len(arg103))
    defer mbTrans104.Close()
    _, err105 := mbTrans104.WriteString(arg103)
    if err105 != nil {
      Usage()
      return
    }
    factory106 := thrift.NewTJSONProtocolFactory()
    jsProt107 := factory106.GetProtocol(mbTrans104)
    argvalue0 := driver.NewIWorkerId()
    err108 := argvalue0.Read(context.Background(), jsProt107)
    if err108 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.Start(context.Background(), value0))
    fmt.Print("\n")
    break
  case "destroy":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Destroy requires 1 args")
      flag.Usage()
    }
    arg109 := flag.Arg(1)
    mbTrans110 := thrift.NewTMemoryBufferLen(len(arg109))
    defer mbTrans110.Close()
    _, err111 := mbTrans110.WriteString(arg109)
    if err111 != nil {
      Usage()
      return
    }
    factory112 := thrift.NewTJSONProtocolFactory()
    jsProt113 := factory112.GetProtocol(mbTrans110)
    argvalue0 := driver.NewIWorkerId()
    err114 := argvalue0.Read(context.Background(), jsProt113)
    if err114 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.Destroy(context.Background(), value0))
    fmt.Print("\n")
    break
  case "newInstance":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "NewInstance_ requires 2 args")
      flag.Usage()
    }
    argvalue0, err115 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err115 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    fmt.Print(client.NewInstance_(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "newInstance3":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "NewInstance3_ requires 3 args")
      flag.Usage()
    }
    argvalue0, err117 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err117 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    argvalue2 := flag.Arg(3)
    value2 := argvalue2
    fmt.Print(client.NewInstance3_(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "newInstance4":
    if flag.NArg() - 1 != 4 {
      fmt.Fprintln(os.Stderr, "NewInstance4_ requires 4 args")
      flag.Usage()
    }
    argvalue0, err120 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err120 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    tmp2, err122 := (strconv.Atoi(flag.Arg(3)))
    if err122 != nil {
      Usage()
      return
    }
    argvalue2 := int32(tmp2)
    value2 := argvalue2
    tmp3, err123 := (strconv.Atoi(flag.Arg(4)))
    if err123 != nil {
      Usage()
      return
    }
    argvalue3 := int32(tmp3)
    value3 := argvalue3
    fmt.Print(client.NewInstance4_(context.Background(), value0, value1, value2, value3))
    fmt.Print("\n")
    break
  case "newInstance5":
    if flag.NArg() - 1 != 5 {
      fmt.Fprintln(os.Stderr, "NewInstance5_ requires 5 args")
      flag.Usage()
    }
    argvalue0, err124 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err124 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    argvalue2 := flag.Arg(3)
    value2 := argvalue2
    tmp3, err127 := (strconv.Atoi(flag.Arg(4)))
    if err127 != nil {
      Usage()
      return
    }
    argvalue3 := int32(tmp3)
    value3 := argvalue3
    tmp4, err128 := (strconv.Atoi(flag.Arg(5)))
    if err128 != nil {
      Usage()
      return
    }
    argvalue4 := int32(tmp4)
    value4 := argvalue4
    fmt.Print(client.NewInstance5_(context.Background(), value0, value1, value2, value3, value4))
    fmt.Print("\n")
    break
  case "setName":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "SetName requires 2 args")
      flag.Usage()
    }
    arg129 := flag.Arg(1)
    mbTrans130 := thrift.NewTMemoryBufferLen(len(arg129))
    defer mbTrans130.Close()
    _, err131 := mbTrans130.WriteString(arg129)
    if err131 != nil {
      Usage()
      return
    }
    factory132 := thrift.NewTJSONProtocolFactory()
    jsProt133 := factory132.GetProtocol(mbTrans130)
    argvalue0 := driver.NewIWorkerId()
    err134 := argvalue0.Read(context.Background(), jsProt133)
    if err134 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    fmt.Print(client.SetName(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "parallelize":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "Parallelize requires 3 args")
      flag.Usage()
    }
    arg136 := flag.Arg(1)
    mbTrans137 := thrift.NewTMemoryBufferLen(len(arg136))
    defer mbTrans137.Close()
    _, err138 := mbTrans137.WriteString(arg136)
    if err138 != nil {
      Usage()
      return
    }
    factory139 := thrift.NewTJSONProtocolFactory()
    jsProt140 := factory139.GetProtocol(mbTrans137)
    argvalue0 := driver.NewIWorkerId()
    err141 := argvalue0.Read(context.Background(), jsProt140)
    if err141 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1, err142 := (strconv.ParseInt(flag.Arg(2), 10, 64))
    if err142 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    argvalue2, err143 := (strconv.ParseInt(flag.Arg(3), 10, 64))
    if err143 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    fmt.Print(client.Parallelize(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "parallelize4":
    if flag.NArg() - 1 != 4 {
      fmt.Fprintln(os.Stderr, "Parallelize4 requires 4 args")
      flag.Usage()
    }
    arg144 := flag.Arg(1)
    mbTrans145 := thrift.NewTMemoryBufferLen(len(arg144))
    defer mbTrans145.Close()
    _, err146 := mbTrans145.WriteString(arg144)
    if err146 != nil {
      Usage()
      return
    }
    factory147 := thrift.NewTJSONProtocolFactory()
    jsProt148 := factory147.GetProtocol(mbTrans145)
    argvalue0 := driver.NewIWorkerId()
    err149 := argvalue0.Read(context.Background(), jsProt148)
    if err149 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1, err150 := (strconv.ParseInt(flag.Arg(2), 10, 64))
    if err150 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    argvalue2, err151 := (strconv.ParseInt(flag.Arg(3), 10, 64))
    if err151 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    arg152 := flag.Arg(4)
    mbTrans153 := thrift.NewTMemoryBufferLen(len(arg152))
    defer mbTrans153.Close()
    _, err154 := mbTrans153.WriteString(arg152)
    if err154 != nil {
      Usage()
      return
    }
    factory155 := thrift.NewTJSONProtocolFactory()
    jsProt156 := factory155.GetProtocol(mbTrans153)
    argvalue3 := rpc.NewISource()
    err157 := argvalue3.Read(context.Background(), jsProt156)
    if err157 != nil {
      Usage()
      return
    }
    value3 := argvalue3
    fmt.Print(client.Parallelize4(context.Background(), value0, value1, value2, value3))
    fmt.Print("\n")
    break
  case "importDataFrame":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "ImportDataFrame requires 2 args")
      flag.Usage()
    }
    arg158 := flag.Arg(1)
    mbTrans159 := thrift.NewTMemoryBufferLen(len(arg158))
    defer mbTrans159.Close()
    _, err160 := mbTrans159.WriteString(arg158)
    if err160 != nil {
      Usage()
      return
    }
    factory161 := thrift.NewTJSONProtocolFactory()
    jsProt162 := factory161.GetProtocol(mbTrans159)
    argvalue0 := driver.NewIWorkerId()
    err163 := argvalue0.Read(context.Background(), jsProt162)
    if err163 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    arg164 := flag.Arg(2)
    mbTrans165 := thrift.NewTMemoryBufferLen(len(arg164))
    defer mbTrans165.Close()
    _, err166 := mbTrans165.WriteString(arg164)
    if err166 != nil {
      Usage()
      return
    }
    factory167 := thrift.NewTJSONProtocolFactory()
    jsProt168 := factory167.GetProtocol(mbTrans165)
    argvalue1 := driver.NewIDataFrameId()
    err169 := argvalue1.Read(context.Background(), jsProt168)
    if err169 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    fmt.Print(client.ImportDataFrame(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "importDataFrame3":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "ImportDataFrame3 requires 3 args")
      flag.Usage()
    }
    arg170 := flag.Arg(1)
    mbTrans171 := thrift.NewTMemoryBufferLen(len(arg170))
    defer mbTrans171.Close()
    _, err172 := mbTrans171.WriteString(arg170)
    if err172 != nil {
      Usage()
      return
    }
    factory173 := thrift.NewTJSONProtocolFactory()
    jsProt174 := factory173.GetProtocol(mbTrans171)
    argvalue0 := driver.NewIWorkerId()
    err175 := argvalue0.Read(context.Background(), jsProt174)
    if err175 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    arg176 := flag.Arg(2)
    mbTrans177 := thrift.NewTMemoryBufferLen(len(arg176))
    defer mbTrans177.Close()
    _, err178 := mbTrans177.WriteString(arg176)
    if err178 != nil {
      Usage()
      return
    }
    factory179 := thrift.NewTJSONProtocolFactory()
    jsProt180 := factory179.GetProtocol(mbTrans177)
    argvalue1 := driver.NewIDataFrameId()
    err181 := argvalue1.Read(context.Background(), jsProt180)
    if err181 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    arg182 := flag.Arg(3)
    mbTrans183 := thrift.NewTMemoryBufferLen(len(arg182))
    defer mbTrans183.Close()
    _, err184 := mbTrans183.WriteString(arg182)
    if err184 != nil {
      Usage()
      return
    }
    factory185 := thrift.NewTJSONProtocolFactory()
    jsProt186 := factory185.GetProtocol(mbTrans183)
    argvalue2 := rpc.NewISource()
    err187 := argvalue2.Read(context.Background(), jsProt186)
    if err187 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    fmt.Print(client.ImportDataFrame3(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "plainFile":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "PlainFile requires 3 args")
      flag.Usage()
    }
    arg188 := flag.Arg(1)
    mbTrans189 := thrift.NewTMemoryBufferLen(len(arg188))
    defer mbTrans189.Close()
    _, err190 := mbTrans189.WriteString(arg188)
    if err190 != nil {
      Usage()
      return
    }
    factory191 := thrift.NewTJSONProtocolFactory()
    jsProt192 := factory191.GetProtocol(mbTrans189)
    argvalue0 := driver.NewIWorkerId()
    err193 := argvalue0.Read(context.Background(), jsProt192)
    if err193 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    tmp2, err195 := (strconv.Atoi(flag.Arg(3)))
    if err195 != nil {
      Usage()
      return
    }
    argvalue2 := int8(tmp2)
    value2 := argvalue2
    fmt.Print(client.PlainFile(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "plainFile4":
    if flag.NArg() - 1 != 4 {
      fmt.Fprintln(os.Stderr, "PlainFile4 requires 4 args")
      flag.Usage()
    }
    arg196 := flag.Arg(1)
    mbTrans197 := thrift.NewTMemoryBufferLen(len(arg196))
    defer mbTrans197.Close()
    _, err198 := mbTrans197.WriteString(arg196)
    if err198 != nil {
      Usage()
      return
    }
    factory199 := thrift.NewTJSONProtocolFactory()
    jsProt200 := factory199.GetProtocol(mbTrans197)
    argvalue0 := driver.NewIWorkerId()
    err201 := argvalue0.Read(context.Background(), jsProt200)
    if err201 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    argvalue2, err203 := (strconv.ParseInt(flag.Arg(3), 10, 64))
    if err203 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    tmp3, err204 := (strconv.Atoi(flag.Arg(4)))
    if err204 != nil {
      Usage()
      return
    }
    argvalue3 := int8(tmp3)
    value3 := argvalue3
    fmt.Print(client.PlainFile4(context.Background(), value0, value1, value2, value3))
    fmt.Print("\n")
    break
  case "textFile":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "TextFile requires 2 args")
      flag.Usage()
    }
    arg205 := flag.Arg(1)
    mbTrans206 := thrift.NewTMemoryBufferLen(len(arg205))
    defer mbTrans206.Close()
    _, err207 := mbTrans206.WriteString(arg205)
    if err207 != nil {
      Usage()
      return
    }
    factory208 := thrift.NewTJSONProtocolFactory()
    jsProt209 := factory208.GetProtocol(mbTrans206)
    argvalue0 := driver.NewIWorkerId()
    err210 := argvalue0.Read(context.Background(), jsProt209)
    if err210 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    fmt.Print(client.TextFile(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "textFile3":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "TextFile3 requires 3 args")
      flag.Usage()
    }
    arg212 := flag.Arg(1)
    mbTrans213 := thrift.NewTMemoryBufferLen(len(arg212))
    defer mbTrans213.Close()
    _, err214 := mbTrans213.WriteString(arg212)
    if err214 != nil {
      Usage()
      return
    }
    factory215 := thrift.NewTJSONProtocolFactory()
    jsProt216 := factory215.GetProtocol(mbTrans213)
    argvalue0 := driver.NewIWorkerId()
    err217 := argvalue0.Read(context.Background(), jsProt216)
    if err217 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    argvalue2, err219 := (strconv.ParseInt(flag.Arg(3), 10, 64))
    if err219 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    fmt.Print(client.TextFile3(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "partitionObjectFile":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "PartitionObjectFile requires 2 args")
      flag.Usage()
    }
    arg220 := flag.Arg(1)
    mbTrans221 := thrift.NewTMemoryBufferLen(len(arg220))
    defer mbTrans221.Close()
    _, err222 := mbTrans221.WriteString(arg220)
    if err222 != nil {
      Usage()
      return
    }
    factory223 := thrift.NewTJSONProtocolFactory()
    jsProt224 := factory223.GetProtocol(mbTrans221)
    argvalue0 := driver.NewIWorkerId()
    err225 := argvalue0.Read(context.Background(), jsProt224)
    if err225 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    fmt.Print(client.PartitionObjectFile(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "partitionObjectFile3":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "PartitionObjectFile3 requires 3 args")
      flag.Usage()
    }
    arg227 := flag.Arg(1)
    mbTrans228 := thrift.NewTMemoryBufferLen(len(arg227))
    defer mbTrans228.Close()
    _, err229 := mbTrans228.WriteString(arg227)
    if err229 != nil {
      Usage()
      return
    }
    factory230 := thrift.NewTJSONProtocolFactory()
    jsProt231 := factory230.GetProtocol(mbTrans228)
    argvalue0 := driver.NewIWorkerId()
    err232 := argvalue0.Read(context.Background(), jsProt231)
    if err232 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    arg234 := flag.Arg(3)
    mbTrans235 := thrift.NewTMemoryBufferLen(len(arg234))
    defer mbTrans235.Close()
    _, err236 := mbTrans235.WriteString(arg234)
    if err236 != nil {
      Usage()
      return
    }
    factory237 := thrift.NewTJSONProtocolFactory()
    jsProt238 := factory237.GetProtocol(mbTrans235)
    argvalue2 := rpc.NewISource()
    err239 := argvalue2.Read(context.Background(), jsProt238)
    if err239 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    fmt.Print(client.PartitionObjectFile3(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "partitionTextFile":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "PartitionTextFile requires 2 args")
      flag.Usage()
    }
    arg240 := flag.Arg(1)
    mbTrans241 := thrift.NewTMemoryBufferLen(len(arg240))
    defer mbTrans241.Close()
    _, err242 := mbTrans241.WriteString(arg240)
    if err242 != nil {
      Usage()
      return
    }
    factory243 := thrift.NewTJSONProtocolFactory()
    jsProt244 := factory243.GetProtocol(mbTrans241)
    argvalue0 := driver.NewIWorkerId()
    err245 := argvalue0.Read(context.Background(), jsProt244)
    if err245 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    fmt.Print(client.PartitionTextFile(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "partitionJsonFile3a":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "PartitionJsonFile3a requires 3 args")
      flag.Usage()
    }
    arg247 := flag.Arg(1)
    mbTrans248 := thrift.NewTMemoryBufferLen(len(arg247))
    defer mbTrans248.Close()
    _, err249 := mbTrans248.WriteString(arg247)
    if err249 != nil {
      Usage()
      return
    }
    factory250 := thrift.NewTJSONProtocolFactory()
    jsProt251 := factory250.GetProtocol(mbTrans248)
    argvalue0 := driver.NewIWorkerId()
    err252 := argvalue0.Read(context.Background(), jsProt251)
    if err252 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    argvalue2 := flag.Arg(3) == "true"
    value2 := argvalue2
    fmt.Print(client.PartitionJsonFile3a(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "partitionJsonFile3b":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "PartitionJsonFile3b requires 3 args")
      flag.Usage()
    }
    arg255 := flag.Arg(1)
    mbTrans256 := thrift.NewTMemoryBufferLen(len(arg255))
    defer mbTrans256.Close()
    _, err257 := mbTrans256.WriteString(arg255)
    if err257 != nil {
      Usage()
      return
    }
    factory258 := thrift.NewTJSONProtocolFactory()
    jsProt259 := factory258.GetProtocol(mbTrans256)
    argvalue0 := driver.NewIWorkerId()
    err260 := argvalue0.Read(context.Background(), jsProt259)
    if err260 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    arg262 := flag.Arg(3)
    mbTrans263 := thrift.NewTMemoryBufferLen(len(arg262))
    defer mbTrans263.Close()
    _, err264 := mbTrans263.WriteString(arg262)
    if err264 != nil {
      Usage()
      return
    }
    factory265 := thrift.NewTJSONProtocolFactory()
    jsProt266 := factory265.GetProtocol(mbTrans263)
    argvalue2 := rpc.NewISource()
    err267 := argvalue2.Read(context.Background(), jsProt266)
    if err267 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    fmt.Print(client.PartitionJsonFile3b(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "loadLibrary":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "LoadLibrary requires 2 args")
      flag.Usage()
    }
    arg268 := flag.Arg(1)
    mbTrans269 := thrift.NewTMemoryBufferLen(len(arg268))
    defer mbTrans269.Close()
    _, err270 := mbTrans269.WriteString(arg268)
    if err270 != nil {
      Usage()
      return
    }
    factory271 := thrift.NewTJSONProtocolFactory()
    jsProt272 := factory271.GetProtocol(mbTrans269)
    argvalue0 := driver.NewIWorkerId()
    err273 := argvalue0.Read(context.Background(), jsProt272)
    if err273 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    fmt.Print(client.LoadLibrary(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "execute":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "Execute requires 2 args")
      flag.Usage()
    }
    arg275 := flag.Arg(1)
    mbTrans276 := thrift.NewTMemoryBufferLen(len(arg275))
    defer mbTrans276.Close()
    _, err277 := mbTrans276.WriteString(arg275)
    if err277 != nil {
      Usage()
      return
    }
    factory278 := thrift.NewTJSONProtocolFactory()
    jsProt279 := factory278.GetProtocol(mbTrans276)
    argvalue0 := driver.NewIWorkerId()
    err280 := argvalue0.Read(context.Background(), jsProt279)
    if err280 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    arg281 := flag.Arg(2)
    mbTrans282 := thrift.NewTMemoryBufferLen(len(arg281))
    defer mbTrans282.Close()
    _, err283 := mbTrans282.WriteString(arg281)
    if err283 != nil {
      Usage()
      return
    }
    factory284 := thrift.NewTJSONProtocolFactory()
    jsProt285 := factory284.GetProtocol(mbTrans282)
    argvalue1 := rpc.NewISource()
    err286 := argvalue1.Read(context.Background(), jsProt285)
    if err286 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    fmt.Print(client.Execute(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "executeTo":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "ExecuteTo requires 2 args")
      flag.Usage()
    }
    arg287 := flag.Arg(1)
    mbTrans288 := thrift.NewTMemoryBufferLen(len(arg287))
    defer mbTrans288.Close()
    _, err289 := mbTrans288.WriteString(arg287)
    if err289 != nil {
      Usage()
      return
    }
    factory290 := thrift.NewTJSONProtocolFactory()
    jsProt291 := factory290.GetProtocol(mbTrans288)
    argvalue0 := driver.NewIWorkerId()
    err292 := argvalue0.Read(context.Background(), jsProt291)
    if err292 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    arg293 := flag.Arg(2)
    mbTrans294 := thrift.NewTMemoryBufferLen(len(arg293))
    defer mbTrans294.Close()
    _, err295 := mbTrans294.WriteString(arg293)
    if err295 != nil {
      Usage()
      return
    }
    factory296 := thrift.NewTJSONProtocolFactory()
    jsProt297 := factory296.GetProtocol(mbTrans294)
    argvalue1 := rpc.NewISource()
    err298 := argvalue1.Read(context.Background(), jsProt297)
    if err298 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    fmt.Print(client.ExecuteTo(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "voidCall":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "VoidCall requires 2 args")
      flag.Usage()
    }
    arg299 := flag.Arg(1)
    mbTrans300 := thrift.NewTMemoryBufferLen(len(arg299))
    defer mbTrans300.Close()
    _, err301 := mbTrans300.WriteString(arg299)
    if err301 != nil {
      Usage()
      return
    }
    factory302 := thrift.NewTJSONProtocolFactory()
    jsProt303 := factory302.GetProtocol(mbTrans300)
    argvalue0 := driver.NewIWorkerId()
    err304 := argvalue0.Read(context.Background(), jsProt303)
    if err304 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    arg305 := flag.Arg(2)
    mbTrans306 := thrift.NewTMemoryBufferLen(len(arg305))
    defer mbTrans306.Close()
    _, err307 := mbTrans306.WriteString(arg305)
    if err307 != nil {
      Usage()
      return
    }
    factory308 := thrift.NewTJSONProtocolFactory()
    jsProt309 := factory308.GetProtocol(mbTrans306)
    argvalue1 := rpc.NewISource()
    err310 := argvalue1.Read(context.Background(), jsProt309)
    if err310 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    fmt.Print(client.VoidCall(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "voidCall3":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "VoidCall3 requires 3 args")
      flag.Usage()
    }
    arg311 := flag.Arg(1)
    mbTrans312 := thrift.NewTMemoryBufferLen(len(arg311))
    defer mbTrans312.Close()
    _, err313 := mbTrans312.WriteString(arg311)
    if err313 != nil {
      Usage()
      return
    }
    factory314 := thrift.NewTJSONProtocolFactory()
    jsProt315 := factory314.GetProtocol(mbTrans312)
    argvalue0 := driver.NewIWorkerId()
    err316 := argvalue0.Read(context.Background(), jsProt315)
    if err316 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    arg317 := flag.Arg(2)
    mbTrans318 := thrift.NewTMemoryBufferLen(len(arg317))
    defer mbTrans318.Close()
    _, err319 := mbTrans318.WriteString(arg317)
    if err319 != nil {
      Usage()
      return
    }
    factory320 := thrift.NewTJSONProtocolFactory()
    jsProt321 := factory320.GetProtocol(mbTrans318)
    argvalue1 := driver.NewIDataFrameId()
    err322 := argvalue1.Read(context.Background(), jsProt321)
    if err322 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    arg323 := flag.Arg(3)
    mbTrans324 := thrift.NewTMemoryBufferLen(len(arg323))
    defer mbTrans324.Close()
    _, err325 := mbTrans324.WriteString(arg323)
    if err325 != nil {
      Usage()
      return
    }
    factory326 := thrift.NewTJSONProtocolFactory()
    jsProt327 := factory326.GetProtocol(mbTrans324)
    argvalue2 := rpc.NewISource()
    err328 := argvalue2.Read(context.Background(), jsProt327)
    if err328 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    fmt.Print(client.VoidCall3(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "call":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "Call requires 2 args")
      flag.Usage()
    }
    arg329 := flag.Arg(1)
    mbTrans330 := thrift.NewTMemoryBufferLen(len(arg329))
    defer mbTrans330.Close()
    _, err331 := mbTrans330.WriteString(arg329)
    if err331 != nil {
      Usage()
      return
    }
    factory332 := thrift.NewTJSONProtocolFactory()
    jsProt333 := factory332.GetProtocol(mbTrans330)
    argvalue0 := driver.NewIWorkerId()
    err334 := argvalue0.Read(context.Background(), jsProt333)
    if err334 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    arg335 := flag.Arg(2)
    mbTrans336 := thrift.NewTMemoryBufferLen(len(arg335))
    defer mbTrans336.Close()
    _, err337 := mbTrans336.WriteString(arg335)
    if err337 != nil {
      Usage()
      return
    }
    factory338 := thrift.NewTJSONProtocolFactory()
    jsProt339 := factory338.GetProtocol(mbTrans336)
    argvalue1 := rpc.NewISource()
    err340 := argvalue1.Read(context.Background(), jsProt339)
    if err340 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    fmt.Print(client.Call(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "call3":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "Call3 requires 3 args")
      flag.Usage()
    }
    arg341 := flag.Arg(1)
    mbTrans342 := thrift.NewTMemoryBufferLen(len(arg341))
    defer mbTrans342.Close()
    _, err343 := mbTrans342.WriteString(arg341)
    if err343 != nil {
      Usage()
      return
    }
    factory344 := thrift.NewTJSONProtocolFactory()
    jsProt345 := factory344.GetProtocol(mbTrans342)
    argvalue0 := driver.NewIWorkerId()
    err346 := argvalue0.Read(context.Background(), jsProt345)
    if err346 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    arg347 := flag.Arg(2)
    mbTrans348 := thrift.NewTMemoryBufferLen(len(arg347))
    defer mbTrans348.Close()
    _, err349 := mbTrans348.WriteString(arg347)
    if err349 != nil {
      Usage()
      return
    }
    factory350 := thrift.NewTJSONProtocolFactory()
    jsProt351 := factory350.GetProtocol(mbTrans348)
    argvalue1 := driver.NewIDataFrameId()
    err352 := argvalue1.Read(context.Background(), jsProt351)
    if err352 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    arg353 := flag.Arg(3)
    mbTrans354 := thrift.NewTMemoryBufferLen(len(arg353))
    defer mbTrans354.Close()
    _, err355 := mbTrans354.WriteString(arg353)
    if err355 != nil {
      Usage()
      return
    }
    factory356 := thrift.NewTJSONProtocolFactory()
    jsProt357 := factory356.GetProtocol(mbTrans354)
    argvalue2 := rpc.NewISource()
    err358 := argvalue2.Read(context.Background(), jsProt357)
    if err358 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    fmt.Print(client.Call3(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "":
    Usage()
    break
  default:
    fmt.Fprintln(os.Stderr, "Invalid function ", cmd)
  }
}
