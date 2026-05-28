//go:build !nogorust

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

// FlightData represents data to be transferred via Flight protocol
type FlightData struct {
	Descriptor *FlightDescriptor
	Data      []byte
	Metadata  map[string]string
}

// FlightDescriptor describes the flight
type FlightDescriptor struct {
	Type FlightDescriptorType
	CMD  []byte
	Path []string
}

// FlightDescriptorType represents the type of descriptor
type FlightDescriptorType int

const (
	FlightDescriptorCMD FlightDescriptorType = iota
	FlightDescriptorPATH
)

// FlightEndpoint represents a flight endpoint
type FlightEndpoint struct {
	Ticket *Ticket
}

// Ticket is a ticket for retrieving data
type Ticket struct {
	Ticket []byte
}

// FlightInfo contains information about a flight
type FlightInfo struct {
	Descriptor *FlightDescriptor
	Endpoints  []*FlightEndpoint
	DataSize   int64
}

// Criteria for listing flights
type Criteria struct {
	Expression []byte
}

// PutResult is the result of a put operation
type PutResult struct {
	AppMetadata []byte
}

// FlightServer implements a gRPC Flight server
type FlightServer struct {
	UnimplementedFlightServiceServer
	mu         sync.RWMutex
	data      map[string]*FlightData
	allocator  interface{}
	schema     *MedicalSchema
}

// UnimplementedFlightServiceServer is for compatibility
type UnimplementedFlightServiceServer struct{}

// NewFlightServer creates a new Flight server
func NewFlightServer() *FlightServer {
	return &FlightServer{
		data:   make(map[string]*FlightData),
		schema: defaultMedicalSchema(),
	}
}

// DoGet retrieves flight data for a ticket
func (s *FlightServer) DoGet(ctx context.Context, ticket *Ticket) (*FlightDataResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.data[string(ticket.Ticket)]
	if !ok {
		return nil, fmt.Errorf("ticket not found: %s", ticket.Ticket)
	}

	return &FlightDataResponse{
		Data:       data.Data,
		Descriptor: data.Descriptor,
	}, nil
}

// DoPut receives flight data
func (s *FlightServer) DoPut(ctx context.Context, stream FlightService_DoPutServer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&PutResult{})
		}
		if err != nil {
			return err
		}

		if req.Descriptor != nil && len(req.Descriptor.CMD) > 0 {
			ticket := string(req.Descriptor.CMD)
			if _, exists := s.data[ticket]; !exists {
				s.data[ticket] = &FlightData{
					Descriptor: req.Descriptor,
					Data:      make([]byte, 0),
					Metadata:  make(map[string]string),
				}
			}
			s.data[ticket].Data = append(s.data[ticket].Data, req.Data...)
		}
	}
}

// ListFlights returns available flights
func (s *FlightServer) ListFlights(req *Criteria, stream FlightService_ListFlightsServer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for name, data := range s.data {
		info := &FlightInfo{
			Descriptor: data.Descriptor,
			Endpoints: []*FlightEndpoint{
				{Ticket: &Ticket{Ticket: []byte(name)}},
			},
			DataSize: int64(len(data.Data)),
		}

		if err := stream.Send(info); err != nil {
			return err
		}
	}

	return nil
}

// GetFlightInfo returns flight information
func (s *FlightServer) GetFlightInfo(ctx context.Context, desc *FlightDescriptor) (*FlightInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.data[string(desc.CMD)]
	if !ok {
		return nil, fmt.Errorf("flight not found")
	}

	return &FlightInfo{
		Descriptor: desc,
		Endpoints: []*FlightEndpoint{
			{Ticket: &Ticket{Ticket: desc.CMD}},
		},
		DataSize: int64(len(data.Data)),
	}, nil
}

// StartFlightServer starts the gRPC Flight server
func StartFlightServer(addr string, port int) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen: %v", err)
	}

	s := grpc.NewServer(
		grpc.Creds(insecure.NewCredentials()),
	)
	RegisterFlightServiceServer(s, NewFlightServer())
	reflection.Register(s)

	go func() {
		if err := s.Serve(lis); err != nil {
			fmt.Printf("Flight server error: %v\n", err)
		}
	}()

	return s, lis, nil
}

// FlightDataResponse is the response for DoGet
type FlightDataResponse struct {
	Data       []byte
	Descriptor *FlightDescriptor
}

// FlightService_DoPutServer is the server interface for DoPut
type FlightService_DoPutServer interface {
	SendAndClose(*PutResult) error
	Recv() (*FlightDataRequest, error)
}

// FlightService_ListFlightsServer is the server interface for ListFlights
type FlightService_ListFlightsServer interface {
	Send(*FlightInfo) error
}

// FlightDataRequest is the request for DoPut
type FlightDataRequest struct {
	Descriptor *FlightDescriptor
	Data       []byte
}

// RegisterFlightServiceServer registers the Flight service
func RegisterFlightServiceServer(s *grpc.Server, srv *FlightServer) {
	s.RegisterService(&_FlightService_serviceDesc, srv)
}

var _ FlightServiceServer = (*FlightServer)(nil)

var _ FlightService_DoPutServer = (*FlightServer)(nil)
var _ FlightService_ListFlightsServer = (*FlightServer)(nil)

var _FlightService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "flight.FlightService",
	HandlerType: (*FlightServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "DoGet",
			Handler:       _FlightServer_DoGet_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "DoPut",
			Handler:       _FlightServer_DoPut_Handler,
			ClientStreams: true,
		},
		{
			StreamName:    "ListFlights",
			Handler:       _FlightServer_ListFlights_Handler,
			ServerStreams: true,
		},
	},
}

func _FlightServer_DoGet_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Ticket)
	if err := dec(in); err != nil {
		return nil, err
	}
	return srv.(*FlightServer).DoGet(ctx, in)
}

func _FlightServer_DoPut_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(*FlightServer).DoPut(stream.Context(), &flightPutStream{stream})
}

type flightPutStream struct {
	grpc.ServerStream
}

func (x *flightPutStream) Recv() (*FlightDataRequest, error) {
	m := new(FlightDataRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _FlightServer_ListFlights_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(Criteria)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(*FlightServer).ListFlights(m, &flightListStream{stream})
}

type flightListStream struct {
	grpc.ServerStream
}

func (x *flightListStream) Send(m *FlightInfo) error {
	return x.ServerStream.SendMsg(m)
}

// FlightServiceServer is the server interface for FlightService
type FlightServiceServer interface {
	DoGet(context.Context, *Ticket) (*FlightDataResponse, error)
	DoPut(context.Context, FlightService_DoPutServer) error
	ListFlights(*Criteria, FlightService_ListFlightsServer) error
	GetFlightInfo(context.Context, *FlightDescriptor) (*FlightInfo, error)
}

// FlightClient is a client for Arrow Flight
type FlightClient struct {
	conn   *grpc.ClientConn
	client FlightServiceClient
}

// NewFlightClient creates a new Arrow Flight client
func NewFlightClient(addr string) (*FlightClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	return &FlightClient{
		conn:   conn,
		client: NewFlightServiceClient(conn),
	}, nil
}

// Close closes the client connection
func (c *FlightClient) Close() error {
	return c.conn.Close()
}

// SendData sends data to the Flight server
func (c *FlightClient) SendData(ticket string, data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := c.client.DoPut(ctx)
	if err != nil {
		return err
	}

	err = stream.Send(&FlightDataRequest{
		Descriptor: &FlightDescriptor{
			Type: FlightDescriptorCMD,
			CMD:  []byte(ticket),
		},
		Data: data,
	})
	if err != nil {
		return err
	}

	_, err = stream.CloseAndRecv()
	return err
}

// ReceiveData receives data from the Flight server
func (c *FlightClient) ReceiveData(ticket string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	response, err := c.client.DoGet(ctx, &Ticket{Ticket: []byte(ticket)})
	if err != nil {
		return nil, err
	}

	return response.Data, nil
}

// FlightServiceClient is the client interface for FlightService
type FlightServiceClient interface {
	DoGet(ctx context.Context, in *Ticket, opts ...grpc.CallOption) (*FlightDataResponse, error)
	DoPut(ctx context.Context, opts ...grpc.CallOption) (FlightService_DoPutClient, error)
}

// NewFlightServiceClient creates a new Flight service client
func NewFlightServiceClient(cc *grpc.ClientConn) FlightServiceClient {
	return &flightServiceClient{cc}
}

type flightServiceClient struct {
	cc *grpc.ClientConn
}

func (c *flightServiceClient) DoGet(ctx context.Context, in *Ticket, opts ...grpc.CallOption) (*FlightDataResponse, error) {
	out := new(FlightDataResponse)
	err := c.cc.Invoke(ctx, "/flight.FlightService/DoGet", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *flightServiceClient) DoPut(ctx context.Context, opts ...grpc.CallOption) (FlightService_DoPutClient, error) {
	stream, err := c.cc.NewStream(ctx, &_FlightService_serviceDesc.Streams[1], "/flight.FlightService/DoPut", opts...)
	if err != nil {
		return nil, err
	}
	x := &flightClientStream{stream}
	return x, nil
}

type FlightService_DoPutClient interface {
	Send(*FlightDataRequest) error
	CloseAndRecv() (*PutResult, error)
}

type flightClientStream struct {
	grpc.ClientStream
}

func (x *flightClientStream) Send(m *FlightDataRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *flightClientStream) CloseAndRecv() (*PutResult, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(PutResult)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// WriteFlightData writes data using Flight protocol
func WriteFlightData(client *FlightClient, ticket string, data []byte) error {
	return client.SendData(ticket, data)
}

// ReadFlightData reads data using Flight protocol
func ReadFlightData(client *FlightClient, ticket string) ([]byte, error) {
	return client.ReceiveData(ticket)
}

// RecordToFlightData converts an Arrow record to Flight data
func RecordToFlightData(record interface{}) ([]byte, error) {
	return json.Marshal(record)
}
