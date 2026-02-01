package main

import (
	"strings"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/axonops/cqlai-node/internal/db"
)

// HostInfo represents a node in the cluster
type HostInfo struct {
	Datacenter          string `json:"datacenter"`
	Rack                string `json:"rack"`
	Address             string `json:"address"`
	IsUp                bool   `json:"is_up"`
	BroadcastRPCAddress string `json:"broadcast_rpc_address"`
	BroadcastRPCPort    int    `json:"broadcast_rpc_port"`
}

// ColumnInfo represents a column in a table
type ColumnInfo struct {
	Name       string `json:"name"`
	CQLType    string `json:"cql_type"`
	Kind       string `json:"kind"` // partition_key, clustering, regular, static
	Position   int    `json:"position"`
	IsReversed bool   `json:"is_reversed"`
	IsStatic   bool   `json:"is_static"`
}

// KeyInfo represents a key column (for primary_key, partition_key, clustering_key arrays)
type KeyInfo struct {
	Name       string `json:"name"`
	CQLType    string `json:"cql_type,omitempty"`
	IsReversed bool   `json:"is_reversed,omitempty"`
	IsStatic   bool   `json:"is_static,omitempty"`
}

// IndexInfo represents an index on a table
type IndexInfo struct {
	Name    string            `json:"name"`
	Kind    string            `json:"kind"`
	Options map[string]string `json:"options"`
}

// TableInfo represents a table in a keyspace
type TableInfo struct {
	Name            string                 `json:"name"`
	PrimaryKey      []KeyInfo              `json:"primary_key"`
	PartitionKey    []KeyInfo              `json:"partition_key"`
	ClusteringKey   []KeyInfo              `json:"clustering_key"`
	Columns         []ColumnInfo           `json:"columns"`
	Indexes         []IndexInfo            `json:"indexes"`
	Triggers        []TriggerInfo          `json:"triggers"`
	Views           []string               `json:"views"`
	Options         map[string]interface{} `json:"options"`
	Virtual         bool                   `json:"virtual"`
	IsCQLCompatible bool                   `json:"is_cql_compatible"`
}

// TriggerInfo represents a trigger on a table
type TriggerInfo struct {
	Name    string                 `json:"name"`
	Options map[string]interface{} `json:"options"`
}

// UserTypeInfo represents a user-defined type
type UserTypeInfo struct {
	Name       string   `json:"name"`
	FieldNames []string `json:"field_names"`
	FieldTypes []string `json:"field_types"`
}

// FunctionInfo represents a user-defined function
type FunctionInfo struct {
	Name              string   `json:"name"`
	ArgumentTypes     []string `json:"argument_types"`
	ArgumentNames     []string `json:"argument_names"`
	ReturnType        string   `json:"return_type"`
	CalledOnNullInput bool     `json:"called_on_null_input"`
	Language          string   `json:"language"`
	Body              string   `json:"body"`
}

// AggregateInfo represents a user-defined aggregate
type AggregateInfo struct {
	Name          string   `json:"name"`
	ArgumentTypes []string `json:"argument_types"`
	StateFunc     string   `json:"state_func"`
	StateType     string   `json:"state_type"`
	FinalFunc     string   `json:"final_func"`
	InitCond      string   `json:"init_cond"`
	ReturnType    string   `json:"return_type"`
}

// ViewInfo represents a materialized view
type ViewInfo struct {
	Name          string       `json:"name"`
	BaseTableName string       `json:"base_table_name"`
	PartitionKey  []KeyInfo    `json:"partition_key"`
	ClusteringKey []KeyInfo    `json:"clustering_key"`
	Columns       []ColumnInfo `json:"columns"`
}

// KeyspaceInfo represents a keyspace with all its contents
type KeyspaceInfo struct {
	Name                string                 `json:"name"`
	Virtual             bool                   `json:"virtual"`
	DurableWrites       bool                   `json:"durable_writes"`
	ReplicationStrategy map[string]interface{} `json:"replication_strategy"`
	Tables              []TableInfo            `json:"tables"`
	UserTypes           []UserTypeInfo         `json:"user_types"`
	Functions           []FunctionInfo         `json:"functions"`
	Aggregates          []AggregateInfo        `json:"aggregates"`
	Views               []ViewInfo             `json:"views"`
	Indexes             []IndexInfo            `json:"indexes"`
}

// ClusterMetadata represents the full cluster metadata
type ClusterMetadata struct {
	ClusterName string         `json:"cluster_name"`
	HostsInfo   []HostInfo     `json:"hosts_info"`
	Partitioner string         `json:"partitioner"`
	Keyspaces   []KeyspaceInfo `json:"keyspaces"`
}

// indexKey is used as a map key for index lookup
type indexKey struct {
	keyspace string
	table    string
}

// GetClusterMetadataFromSession extracts full cluster metadata using gocql's built-in metadata API
// This leverages gocql's internal metadata caching for optimal performance
func GetClusterMetadataFromSession(session *db.Session) (*ClusterMetadata, error) {
	metadata := &ClusterMetadata{
		HostsInfo: []HostInfo{},
		Keyspaces: []KeyspaceInfo{},
	}

	// Get cluster name and partitioner from system.local
	var clusterName, partitioner string
	if err := session.Query("SELECT cluster_name, partitioner FROM system.local").Scan(&clusterName, &partitioner); err != nil {
		return nil, err
	}
	metadata.ClusterName = clusterName
	metadata.Partitioner = partitioner

	// Get hosts info
	if err := getHostsInfo(session, metadata); err != nil {
		return nil, err
	}

	// Get all keyspaces using gocql's metadata API
	if err := getKeyspacesUsingMetadataAPI(session, metadata); err != nil {
		return nil, err
	}

	return metadata, nil
}

func getHostsInfo(session *db.Session, metadata *ClusterMetadata) error {
	// Get local node info
	var datacenter, rack, rpcAddress string
	var rpcPort int
	err := session.Query("SELECT data_center, rack, rpc_address, rpc_port FROM system.local").Scan(&datacenter, &rack, &rpcAddress, &rpcPort)
	if err != nil {
		// Try without rpc_port for older Cassandra versions
		err = session.Query("SELECT data_center, rack, rpc_address FROM system.local").Scan(&datacenter, &rack, &rpcAddress)
		if err != nil {
			return err
		}
		rpcPort = 9042
	}

	metadata.HostsInfo = append(metadata.HostsInfo, HostInfo{
		Datacenter:          datacenter,
		Rack:                rack,
		Address:             rpcAddress,
		IsUp:                true,
		BroadcastRPCAddress: rpcAddress,
		BroadcastRPCPort:    rpcPort,
	})

	// Get peer nodes
	iter := session.Query("SELECT peer, data_center, rack, rpc_address FROM system.peers").Iter()
	var peerAddr, peerDC, peerRack, peerRPC string
	for iter.Scan(&peerAddr, &peerDC, &peerRack, &peerRPC) {
		rpc := peerRPC
		if rpc == "" {
			rpc = peerAddr
		}
		metadata.HostsInfo = append(metadata.HostsInfo, HostInfo{
			Datacenter:          peerDC,
			Rack:                peerRack,
			Address:             peerAddr,
			IsUp:                true,
			BroadcastRPCAddress: rpc,
			BroadcastRPCPort:    9042,
		})
	}
	iter.Close()

	return nil
}

// getKeyspacesUsingMetadataAPI uses gocql's built-in metadata caching
// Combined with supplementary queries for indexes and triggers (not in gocql metadata)
func getKeyspacesUsingMetadataAPI(session *db.Session, metadata *ClusterMetadata) error {
	// Get list of all keyspaces (1 query)
	keyspaceNames := []string{}
	ksIter := session.Query("SELECT keyspace_name FROM system_schema.keyspaces").Iter()
	var ksName string
	for ksIter.Scan(&ksName) {
		keyspaceNames = append(keyspaceNames, ksName)
	}
	ksIter.Close()

	// Also get virtual keyspaces
	virtualKeyspaces := make(map[string]bool)
	virtualKsIter := session.Query("SELECT keyspace_name FROM system_virtual_schema.keyspaces").Iter()
	for virtualKsIter.Scan(&ksName) {
		keyspaceNames = append(keyspaceNames, ksName)
		virtualKeyspaces[ksName] = true
	}
	virtualKsIter.Close()

	// Pre-fetch all indexes (gocql metadata doesn't include them)
	indexMap := make(map[indexKey][]IndexInfo)
	idxIter := session.Query("SELECT keyspace_name, table_name, index_name, kind, options FROM system_schema.indexes").Iter()
	var idxKs, idxTable, idxName, idxKind string
	var idxOptions map[string]string
	for idxIter.Scan(&idxKs, &idxTable, &idxName, &idxKind, &idxOptions) {
		key := indexKey{keyspace: idxKs, table: idxTable}
		indexMap[key] = append(indexMap[key], IndexInfo{
			Name:    idxName,
			Kind:    idxKind,
			Options: idxOptions,
		})
	}
	idxIter.Close()

	// Pre-fetch all triggers
	triggerMap := make(map[indexKey][]TriggerInfo)
	trigIter := session.Query("SELECT keyspace_name, table_name, trigger_name, options FROM system_schema.triggers").Iter()
	var trigKs, trigTable, trigName string
	var trigOptions map[string]string
	for trigIter.Scan(&trigKs, &trigTable, &trigName, &trigOptions) {
		key := indexKey{keyspace: trigKs, table: trigTable}
		optionsInterface := make(map[string]interface{})
		for k, v := range trigOptions {
			optionsInterface[k] = v
		}
		triggerMap[key] = append(triggerMap[key], TriggerInfo{
			Name:    trigName,
			Options: optionsInterface,
		})
	}
	trigIter.Close()

	// Pre-fetch virtual tables metadata (gocql doesn't support virtual schema)
	virtualTables := make(map[string][]TableInfo)
	virtualColumns := make(map[indexKey][]ColumnInfo)

	vtIter := session.Query("SELECT keyspace_name, table_name, comment FROM system_virtual_schema.tables").Iter()
	var vtKs, vtTable, vtComment string
	for vtIter.Scan(&vtKs, &vtTable, &vtComment) {
		virtualTables[vtKs] = append(virtualTables[vtKs], TableInfo{
			Name:            vtTable,
			Virtual:         true,
			IsCQLCompatible: false,
			PrimaryKey:      []KeyInfo{},
			PartitionKey:    []KeyInfo{},
			ClusteringKey:   []KeyInfo{},
			Columns:         []ColumnInfo{},
			Indexes:         []IndexInfo{},
			Triggers:        []TriggerInfo{},
			Views:           []string{},
			Options:         make(map[string]interface{}),
		})
	}
	vtIter.Close()

	vcIter := session.Query("SELECT keyspace_name, table_name, column_name, type, kind, position FROM system_virtual_schema.columns").Iter()
	var vcKs, vcTable, vcName, vcType, vcKind string
	var vcPos int
	for vcIter.Scan(&vcKs, &vcTable, &vcName, &vcType, &vcKind, &vcPos) {
		key := indexKey{keyspace: vcKs, table: vcTable}
		virtualColumns[key] = append(virtualColumns[key], ColumnInfo{
			Name:     vcName,
			CQLType:  vcType,
			Kind:     vcKind,
			Position: vcPos,
		})
	}
	vcIter.Close()

	// Populate virtual table columns and keys
	for ksName, tables := range virtualTables {
		for i := range tables {
			key := indexKey{keyspace: ksName, table: tables[i].Name}
			if cols, ok := virtualColumns[key]; ok {
				tables[i].Columns = cols
				// Build primary key info from columns
				for _, col := range cols {
					keyInfo := KeyInfo{Name: col.Name, CQLType: col.CQLType}
					if col.Kind == "partition_key" {
						tables[i].PartitionKey = append(tables[i].PartitionKey, keyInfo)
						tables[i].PrimaryKey = append(tables[i].PrimaryKey, keyInfo)
					} else if col.Kind == "clustering" {
						tables[i].ClusteringKey = append(tables[i].ClusteringKey, keyInfo)
						tables[i].PrimaryKey = append(tables[i].PrimaryKey, keyInfo)
					}
				}
			}
		}
	}

	// For each keyspace, use gocql's KeyspaceMetadata which uses internal cache
	for _, ksName := range keyspaceNames {
		isVirtual := virtualKeyspaces[ksName]

		if isVirtual {
			// Virtual keyspaces are not supported by gocql's metadata API
			// Build KeyspaceInfo manually from pre-fetched data
			ksInfo := KeyspaceInfo{
				Name:                ksName,
				Virtual:             true,
				DurableWrites:       false,
				ReplicationStrategy: map[string]interface{}{"class": "LocalStrategy"},
				Tables:              virtualTables[ksName],
				UserTypes:           []UserTypeInfo{},
				Functions:           []FunctionInfo{},
				Aggregates:          []AggregateInfo{},
				Views:               []ViewInfo{},
				Indexes:             []IndexInfo{},
			}
			metadata.Keyspaces = append(metadata.Keyspaces, ksInfo)
			continue
		}

		// Use gocql's metadata API - this leverages internal caching
		ksMeta, err := session.KeyspaceMetadata(ksName)
		if err != nil {
			// Skip keyspaces we can't access (permissions, etc.)
			continue
		}

		ksInfo := convertKeyspaceMetadata(ksMeta, isVirtual, indexMap, triggerMap)
		metadata.Keyspaces = append(metadata.Keyspaces, ksInfo)
	}

	return nil
}

// convertKeyspaceMetadata converts gocql.KeyspaceMetadata to our KeyspaceInfo format
func convertKeyspaceMetadata(ksMeta *gocql.KeyspaceMetadata, isVirtual bool, indexMap map[indexKey][]IndexInfo, triggerMap map[indexKey][]TriggerInfo) KeyspaceInfo {
	ks := KeyspaceInfo{
		Name:                ksMeta.Name,
		Virtual:             isVirtual,
		DurableWrites:       ksMeta.DurableWrites,
		ReplicationStrategy: make(map[string]interface{}),
		Tables:              []TableInfo{},
		UserTypes:           []UserTypeInfo{},
		Functions:           []FunctionInfo{},
		Aggregates:          []AggregateInfo{},
		Views:               []ViewInfo{},
		Indexes:             []IndexInfo{},
	}

	// Convert replication strategy
	if ksMeta.StrategyClass != "" {
		ks.ReplicationStrategy["class"] = ksMeta.StrategyClass
	}
	for k, v := range ksMeta.StrategyOptions {
		ks.ReplicationStrategy[k] = v
	}

	// Convert tables
	for _, tableMeta := range ksMeta.Tables {
		tableInfo := convertTableMetadata(ksMeta.Name, tableMeta, isVirtual, indexMap, triggerMap)
		ks.Tables = append(ks.Tables, tableInfo)
	}

	// Convert user types
	for _, udtMeta := range ksMeta.UserTypes {
		udtInfo := convertUserTypeMetadata(udtMeta)
		ks.UserTypes = append(ks.UserTypes, udtInfo)
	}

	// Convert functions
	for _, funcMeta := range ksMeta.Functions {
		funcInfo := convertFunctionMetadata(funcMeta)
		ks.Functions = append(ks.Functions, funcInfo)
	}

	// Convert aggregates
	for _, aggMeta := range ksMeta.Aggregates {
		aggInfo := convertAggregateMetadata(aggMeta)
		ks.Aggregates = append(ks.Aggregates, aggInfo)
	}

	// Convert materialized views
	for _, mvMeta := range ksMeta.MaterializedViews {
		viewInfo := convertMaterializedViewMetadata(mvMeta)
		ks.Views = append(ks.Views, viewInfo)
	}

	return ks
}

// convertTableMetadata converts gocql.TableMetadata to our TableInfo format
func convertTableMetadata(keyspace string, tableMeta *gocql.TableMetadata, isVirtual bool, indexMap map[indexKey][]IndexInfo, triggerMap map[indexKey][]TriggerInfo) TableInfo {
	table := TableInfo{
		Name:            tableMeta.Name,
		PrimaryKey:      []KeyInfo{},
		PartitionKey:    []KeyInfo{},
		ClusteringKey:   []KeyInfo{},
		Columns:         []ColumnInfo{},
		Indexes:         []IndexInfo{},
		Triggers:        []TriggerInfo{},
		Views:           []string{},
		Options:         make(map[string]interface{}),
		Virtual:         isVirtual,
		IsCQLCompatible: true,
	}

	// Convert partition key
	for _, col := range tableMeta.PartitionKey {
		keyInfo := KeyInfo{
			Name:    col.Name,
			CQLType: formatTypeInfo(col.Type),
		}
		table.PartitionKey = append(table.PartitionKey, keyInfo)
		table.PrimaryKey = append(table.PrimaryKey, keyInfo)
	}

	// Convert clustering key
	for _, col := range tableMeta.ClusteringColumns {
		keyInfo := KeyInfo{
			Name:       col.Name,
			CQLType:    formatTypeInfo(col.Type),
			IsReversed: col.ClusteringOrder == "desc",
		}
		table.ClusteringKey = append(table.ClusteringKey, keyInfo)
		table.PrimaryKey = append(table.PrimaryKey, keyInfo)
	}

	// Convert all columns
	for _, col := range tableMeta.Columns {
		kind := "regular"
		position := -1

		// Determine column kind and position
		for i, pk := range tableMeta.PartitionKey {
			if pk.Name == col.Name {
				kind = "partition_key"
				position = i
				break
			}
		}
		if kind == "regular" {
			for i, ck := range tableMeta.ClusteringColumns {
				if ck.Name == col.Name {
					kind = "clustering"
					position = i
					break
				}
			}
		}

		colInfo := ColumnInfo{
			Name:     col.Name,
			CQLType:  formatTypeInfo(col.Type),
			Kind:     kind,
			Position: position,
		}
		table.Columns = append(table.Columns, colInfo)
	}

	// Add indexes from pre-fetched map
	key := indexKey{keyspace: keyspace, table: tableMeta.Name}
	if indexes, ok := indexMap[key]; ok {
		table.Indexes = indexes
	}

	// Add triggers from pre-fetched map
	if triggers, ok := triggerMap[key]; ok {
		table.Triggers = triggers
	}

	return table
}

// convertUserTypeMetadata converts gocql.UserTypeMetadata to our UserTypeInfo format
func convertUserTypeMetadata(udtMeta *gocql.UserTypeMetadata) UserTypeInfo {
	fieldTypes := make([]string, len(udtMeta.FieldTypes))
	for i, ft := range udtMeta.FieldTypes {
		fieldTypes[i] = formatTypeInfo(ft)
	}

	return UserTypeInfo{
		Name:       udtMeta.Name,
		FieldNames: udtMeta.FieldNames,
		FieldTypes: fieldTypes,
	}
}

// convertFunctionMetadata converts gocql.FunctionMetadata to our FunctionInfo format
func convertFunctionMetadata(funcMeta *gocql.FunctionMetadata) FunctionInfo {
	argTypes := make([]string, len(funcMeta.ArgumentTypes))
	for i, at := range funcMeta.ArgumentTypes {
		argTypes[i] = formatTypeInfo(at)
	}

	return FunctionInfo{
		Name:              funcMeta.Name,
		ArgumentTypes:     argTypes,
		ArgumentNames:     funcMeta.ArgumentNames,
		ReturnType:        formatTypeInfo(funcMeta.ReturnType),
		CalledOnNullInput: funcMeta.CalledOnNullInput,
		Language:          funcMeta.Language,
		Body:              funcMeta.Body,
	}
}

// convertAggregateMetadata converts gocql.AggregateMetadata to our AggregateInfo format
func convertAggregateMetadata(aggMeta *gocql.AggregateMetadata) AggregateInfo {
	argTypes := make([]string, len(aggMeta.ArgumentTypes))
	for i, at := range aggMeta.ArgumentTypes {
		argTypes[i] = formatTypeInfo(at)
	}

	return AggregateInfo{
		Name:          aggMeta.Name,
		ArgumentTypes: argTypes,
		StateFunc:     aggMeta.StateFunc.Name,
		StateType:     formatTypeInfo(aggMeta.StateType),
		FinalFunc:     aggMeta.FinalFunc.Name,
		InitCond:      aggMeta.InitCond,
		ReturnType:    formatTypeInfo(aggMeta.ReturnType),
	}
}

// convertMaterializedViewMetadata converts gocql.MaterializedViewMetadata to our ViewInfo format
// Note: gocql's MV metadata has limited public fields, so we use what's available
func convertMaterializedViewMetadata(mvMeta *gocql.MaterializedViewMetadata) ViewInfo {
	view := ViewInfo{
		Name:          mvMeta.Name,
		PartitionKey:  []KeyInfo{},
		ClusteringKey: []KeyInfo{},
		Columns:       []ColumnInfo{},
	}

	// Get base table name from the BaseTable reference if available
	if mvMeta.BaseTable != nil {
		view.BaseTableName = mvMeta.BaseTable.Name
	}

	return view
}

// formatTypeInfo converts gocql.TypeInfo to a string representation
func formatTypeInfo(typeInfo gocql.TypeInfo) string {
	if typeInfo == nil {
		return "unknown"
	}

	baseType := typeInfo.Type()

	switch baseType {
	case gocql.TypeList, gocql.TypeSet, gocql.TypeMap:
		if collType, ok := typeInfo.(gocql.CollectionType); ok {
			switch collType.Type() {
			case gocql.TypeList:
				return "list<" + formatTypeInfo(collType.Elem) + ">"
			case gocql.TypeSet:
				return "set<" + formatTypeInfo(collType.Elem) + ">"
			case gocql.TypeMap:
				return "map<" + formatTypeInfo(collType.Key) + ", " + formatTypeInfo(collType.Elem) + ">"
			}
		}
	case gocql.TypeTuple:
		if tupleType, ok := typeInfo.(gocql.TupleTypeInfo); ok {
			var elements []string
			for _, elem := range tupleType.Elems {
				elements = append(elements, formatTypeInfo(elem))
			}
			return "tuple<" + strings.Join(elements, ", ") + ">"
		}
	case gocql.TypeUDT:
		if udtType, ok := typeInfo.(gocql.UDTTypeInfo); ok {
			if udtType.Keyspace != "" {
				return udtType.Keyspace + "." + udtType.Name
			}
			return udtType.Name
		}
	}

	return typeNameFromType(baseType)
}

// typeNameFromType converts gocql.Type to string name
func typeNameFromType(t gocql.Type) string {
	switch t {
	case gocql.TypeCustom:
		return "custom"
	case gocql.TypeAscii:
		return "ascii"
	case gocql.TypeBigInt:
		return "bigint"
	case gocql.TypeBlob:
		return "blob"
	case gocql.TypeBoolean:
		return "boolean"
	case gocql.TypeCounter:
		return "counter"
	case gocql.TypeDecimal:
		return "decimal"
	case gocql.TypeDouble:
		return "double"
	case gocql.TypeFloat:
		return "float"
	case gocql.TypeInt:
		return "int"
	case gocql.TypeText:
		return "text"
	case gocql.TypeTimestamp:
		return "timestamp"
	case gocql.TypeUUID:
		return "uuid"
	case gocql.TypeVarchar:
		return "varchar"
	case gocql.TypeVarint:
		return "varint"
	case gocql.TypeTimeUUID:
		return "timeuuid"
	case gocql.TypeInet:
		return "inet"
	case gocql.TypeDate:
		return "date"
	case gocql.TypeDuration:
		return "duration"
	case gocql.TypeTime:
		return "time"
	case gocql.TypeSmallInt:
		return "smallint"
	case gocql.TypeTinyInt:
		return "tinyint"
	case gocql.TypeList:
		return "list"
	case gocql.TypeMap:
		return "map"
	case gocql.TypeSet:
		return "set"
	case gocql.TypeTuple:
		return "tuple"
	case gocql.TypeUDT:
		return "udt"
	default:
		return "unknown"
	}
}
