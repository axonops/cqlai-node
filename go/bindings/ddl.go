package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// tableKey is used as a map key for table-level metadata
type tableKey struct {
	keyspace string
	table    string
}

// ddlMetadataCache holds pre-fetched metadata for batch DDL generation
type ddlMetadataCache struct {
	keyspaces  map[string]ddlKeyspaceInfo
	tables     map[string][]ddlTableInfo      // keyspace -> tables
	columns    map[tableKey][]ddlColumnInfo   // keyspace.table -> columns
	indexes    map[tableKey][]ddlIndexInfo    // keyspace.table -> indexes
	types      map[string][]ddlTypeInfo       // keyspace -> types
	functions  map[string][]ddlFunctionInfo   // keyspace -> functions
	aggregates map[string][]ddlAggregateInfo  // keyspace -> aggregates
	views      map[string][]ddlViewInfo       // keyspace -> views
}

// DDLResult holds the generated DDL statements
type DDLResult struct {
	DDL   string `json:"ddl"`
	Scope string `json:"scope"`
}

// GenerateDDLWithOptions generates DDL statements based on DDLOptions
// Options:
//   - cluster: true - all keyspaces
//   - includeSystem: true - include system keyspaces in cluster DDL
//   - keyspace: "ks_name" - specific keyspace with all objects
//   - keyspace + table: specific table
//   - keyspace + table + index: specific index
//   - keyspace + type: specific user type
//   - keyspace + function: specific function
//   - keyspace + aggregate: specific aggregate
//   - keyspace + view: specific materialized view
func GenerateDDLWithOptions(session *gocql.Session, opts DDLOptions) (*DDLResult, error) {
	// Cluster-level DDL
	if opts.Cluster {
		return generateClusterDDL(session, opts.IncludeSystem)
	}

	// Keyspace is required for non-cluster operations
	if opts.Keyspace == "" {
		return nil, fmt.Errorf("keyspace is required when cluster is false")
	}

	// Table with optional index
	if opts.Table != "" {
		if opts.Index != "" {
			return generateIndexDDL(session, opts.Keyspace, opts.Table, opts.Index)
		}
		return generateTableDDL(session, opts.Keyspace, opts.Table)
	}

	// User type
	if opts.Type != "" {
		return generateTypeDDL(session, opts.Keyspace, opts.Type)
	}

	// Function
	if opts.Function != "" {
		return generateFunctionDDL(session, opts.Keyspace, opts.Function)
	}

	// Aggregate
	if opts.Aggregate != "" {
		return generateAggregateDDL(session, opts.Keyspace, opts.Aggregate)
	}

	// Materialized view
	if opts.View != "" {
		return generateViewDDL(session, opts.Keyspace, opts.View)
	}

	// Just keyspace
	return generateKeyspaceDDL(session, opts.Keyspace)
}

// GenerateDDL generates DDL statements based on scope (legacy string format)
// Deprecated: Use GenerateDDLWithOptions instead
// scope formats:
//   - "cluster" - all keyspaces
//   - "keyspace>ks_name" - specific keyspace with all objects
//   - "keyspace>ks_name>table>tbl_name" - specific table
//   - "keyspace>ks_name>table>tbl_name>index>idx_name" - specific index
//   - "keyspace>ks_name>type>type_name" - specific user type
//   - "keyspace>ks_name>function>func_name" - specific function
//   - "keyspace>ks_name>aggregate>agg_name" - specific aggregate
//   - "keyspace>ks_name>view>view_name" - specific materialized view
func GenerateDDL(session *gocql.Session, scope string) (*DDLResult, error) {
	parts := strings.Split(scope, ">")

	switch parts[0] {
	case "cluster":
		return generateClusterDDL(session, true)
	case "keyspace":
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid scope: keyspace name required")
		}
		ksName := parts[1]

		if len(parts) == 2 {
			return generateKeyspaceDDL(session, ksName)
		}

		if len(parts) < 4 {
			return nil, fmt.Errorf("invalid scope format")
		}

		objectType := parts[2]
		objectName := parts[3]

		switch objectType {
		case "table":
			if len(parts) == 4 {
				return generateTableDDL(session, ksName, objectName)
			}
			if len(parts) == 6 && parts[4] == "index" {
				return generateIndexDDL(session, ksName, objectName, parts[5])
			}
			return nil, fmt.Errorf("invalid table scope format")
		case "type":
			return generateTypeDDL(session, ksName, objectName)
		case "function":
			return generateFunctionDDL(session, ksName, objectName)
		case "aggregate":
			return generateAggregateDDL(session, ksName, objectName)
		case "view":
			return generateViewDDL(session, ksName, objectName)
		default:
			return nil, fmt.Errorf("unknown object type: %s", objectType)
		}
	default:
		return nil, fmt.Errorf("invalid scope: must start with 'cluster' or 'keyspace'")
	}
}

// loadAllMetadata fetches all schema metadata in batch queries
// This reduces N+1 queries to ~10 queries total for the entire cluster
func loadAllMetadata(session *gocql.Session, includeSystem bool) (*ddlMetadataCache, error) {
	cache := &ddlMetadataCache{
		keyspaces:  make(map[string]ddlKeyspaceInfo),
		tables:     make(map[string][]ddlTableInfo),
		columns:    make(map[tableKey][]ddlColumnInfo),
		indexes:    make(map[tableKey][]ddlIndexInfo),
		types:      make(map[string][]ddlTypeInfo),
		functions:  make(map[string][]ddlFunctionInfo),
		aggregates: make(map[string][]ddlAggregateInfo),
		views:      make(map[string][]ddlViewInfo),
	}

	// 1. Fetch ALL keyspaces from system_schema
	iter := session.Query("SELECT keyspace_name, replication, durable_writes FROM system_schema.keyspaces").Iter()
	var ksName string
	var replication map[string]string
	var durableWrites bool
	for iter.Scan(&ksName, &replication, &durableWrites) {
		if !includeSystem && isSystemKeyspace(ksName) {
			continue
		}
		// Make a copy of replication map since gocql reuses the map
		repCopy := make(map[string]string, len(replication))
		for k, v := range replication {
			repCopy[k] = v
		}
		cache.keyspaces[ksName] = ddlKeyspaceInfo{
			Name:          ksName,
			Replication:   repCopy,
			DurableWrites: durableWrites,
		}
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to fetch keyspaces: %v", err)
	}

	// 1b. Fetch virtual keyspaces if includeSystem is true
	if includeSystem {
		iter = session.Query("SELECT keyspace_name FROM system_virtual_schema.keyspaces").Iter()
		for iter.Scan(&ksName) {
			// Virtual keyspaces don't have replication settings
			cache.keyspaces[ksName] = ddlKeyspaceInfo{
				Name:          ksName,
				Replication:   nil, // Virtual keyspaces have no replication
				DurableWrites: false,
				IsVirtual:     true,
			}
		}
		if err := iter.Close(); err != nil {
			// Ignore error - virtual schema may not exist in older Cassandra versions
		}
	}

	// 2. Fetch ALL tables from system_schema
	iter = session.Query("SELECT keyspace_name, table_name, comment FROM system_schema.tables").Iter()
	var tableName, comment string
	for iter.Scan(&ksName, &tableName, &comment) {
		if _, ok := cache.keyspaces[ksName]; !ok {
			continue // Skip tables from excluded keyspaces
		}
		cache.tables[ksName] = append(cache.tables[ksName], ddlTableInfo{
			Name:    tableName,
			Comment: comment,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to fetch tables: %v", err)
	}

	// 2b. Fetch virtual tables if includeSystem is true
	if includeSystem {
		iter = session.Query("SELECT keyspace_name, table_name, comment FROM system_virtual_schema.tables").Iter()
		for iter.Scan(&ksName, &tableName, &comment) {
			if _, ok := cache.keyspaces[ksName]; !ok {
				continue
			}
			cache.tables[ksName] = append(cache.tables[ksName], ddlTableInfo{
				Name:      tableName,
				Comment:   comment,
				IsVirtual: true,
			})
		}
		if err := iter.Close(); err != nil {
			// Ignore error - virtual schema may not exist in older Cassandra versions
		}
	}

	// Sort tables within each keyspace
	for ks := range cache.tables {
		sort.Slice(cache.tables[ks], func(i, j int) bool {
			return cache.tables[ks][i].Name < cache.tables[ks][j].Name
		})
	}

	// 3. Fetch ALL columns from system_schema (includes clustering_order for table options)
	iter = session.Query(`SELECT keyspace_name, table_name, column_name, type, kind, position, clustering_order
		FROM system_schema.columns`).Iter()
	var colName, colType, kind, clusteringOrder string
	var position int
	// Track clustering columns per table for building CLUSTERING ORDER BY
	clusteringCols := make(map[tableKey][]struct {
		name     string
		order    string
		position int
	})
	for iter.Scan(&ksName, &tableName, &colName, &colType, &kind, &position, &clusteringOrder) {
		if _, ok := cache.keyspaces[ksName]; !ok {
			continue
		}
		key := tableKey{keyspace: ksName, table: tableName}
		cache.columns[key] = append(cache.columns[key], ddlColumnInfo{
			Name:            colName,
			Type:            colType,
			Kind:            kind,
			Position:        position,
			ClusteringOrder: clusteringOrder,
		})
		// Track clustering columns for table options
		if kind == "clustering" && clusteringOrder != "" && clusteringOrder != "none" {
			clusteringCols[key] = append(clusteringCols[key], struct {
				name     string
				order    string
				position int
			}{colName, clusteringOrder, position})
		}
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to fetch columns: %v", err)
	}

	// 3b. Fetch virtual table columns if includeSystem is true
	if includeSystem {
		iter = session.Query(`SELECT keyspace_name, table_name, column_name, type, kind, position, clustering_order
			FROM system_virtual_schema.columns`).Iter()
		for iter.Scan(&ksName, &tableName, &colName, &colType, &kind, &position, &clusteringOrder) {
			if _, ok := cache.keyspaces[ksName]; !ok {
				continue
			}
			key := tableKey{keyspace: ksName, table: tableName}
			cache.columns[key] = append(cache.columns[key], ddlColumnInfo{
				Name:            colName,
				Type:            colType,
				Kind:            kind,
				Position:        position,
				ClusteringOrder: clusteringOrder,
			})
			if kind == "clustering" && clusteringOrder != "" && clusteringOrder != "none" {
				clusteringCols[key] = append(clusteringCols[key], struct {
					name     string
					order    string
					position int
				}{colName, clusteringOrder, position})
			}
		}
		if err := iter.Close(); err != nil {
			// Ignore error - virtual schema may not exist in older Cassandra versions
		}
	}

	// Update table info with clustering order from columns
	for ks, tables := range cache.tables {
		for i := range tables {
			key := tableKey{keyspace: ks, table: tables[i].Name}
			if cols, ok := clusteringCols[key]; ok && len(cols) > 0 {
				// Sort by position
				sort.Slice(cols, func(a, b int) bool {
					return cols[a].position < cols[b].position
				})
				var orderParts []string
				for _, c := range cols {
					orderParts = append(orderParts, fmt.Sprintf("%s %s", quoteIdentifier(c.name), c.order))
				}
				cache.tables[ks][i].ClusteringOrder = strings.Join(orderParts, ", ")
			}
		}
	}

	// 4. Fetch ALL indexes
	iter = session.Query("SELECT keyspace_name, table_name, index_name, kind, options FROM system_schema.indexes").Iter()
	var indexName, indexKind string
	var options map[string]string
	for iter.Scan(&ksName, &tableName, &indexName, &indexKind, &options) {
		if _, ok := cache.keyspaces[ksName]; !ok {
			continue
		}
		key := tableKey{keyspace: ksName, table: tableName}
		// Make a copy of options map
		optsCopy := make(map[string]string, len(options))
		for k, v := range options {
			optsCopy[k] = v
		}
		cache.indexes[key] = append(cache.indexes[key], ddlIndexInfo{
			Name:    indexName,
			Kind:    indexKind,
			Options: optsCopy,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to fetch indexes: %v", err)
	}
	// Sort indexes within each table
	for key := range cache.indexes {
		sort.Slice(cache.indexes[key], func(i, j int) bool {
			return cache.indexes[key][i].Name < cache.indexes[key][j].Name
		})
	}

	// 5. Fetch ALL types
	iter = session.Query("SELECT keyspace_name, type_name, field_names, field_types FROM system_schema.types").Iter()
	var typeName string
	var fields, fieldTypes []string
	for iter.Scan(&ksName, &typeName, &fields, &fieldTypes) {
		if _, ok := cache.keyspaces[ksName]; !ok {
			continue
		}
		// Make copies of slices
		fieldsCopy := make([]string, len(fields))
		copy(fieldsCopy, fields)
		typesCopy := make([]string, len(fieldTypes))
		copy(typesCopy, fieldTypes)
		cache.types[ksName] = append(cache.types[ksName], ddlTypeInfo{
			Name:   typeName,
			Fields: fieldsCopy,
			Types:  typesCopy,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to fetch types: %v", err)
	}
	// Sort types within each keyspace
	for ks := range cache.types {
		sort.Slice(cache.types[ks], func(i, j int) bool {
			return cache.types[ks][i].Name < cache.types[ks][j].Name
		})
	}

	// 6. Fetch ALL functions
	iter = session.Query(`SELECT keyspace_name, function_name, argument_names, argument_types,
		return_type, language, body, called_on_null_input FROM system_schema.functions`).Iter()
	var funcName, returnType, language, body string
	var argNames, argTypes []string
	var calledOnNull bool
	for iter.Scan(&ksName, &funcName, &argNames, &argTypes, &returnType, &language, &body, &calledOnNull) {
		if _, ok := cache.keyspaces[ksName]; !ok {
			continue
		}
		argNamesCopy := make([]string, len(argNames))
		copy(argNamesCopy, argNames)
		argTypesCopy := make([]string, len(argTypes))
		copy(argTypesCopy, argTypes)
		cache.functions[ksName] = append(cache.functions[ksName], ddlFunctionInfo{
			Name:              funcName,
			ArgumentNames:     argNamesCopy,
			ArgumentTypes:     argTypesCopy,
			ReturnType:        returnType,
			Language:          language,
			Body:              body,
			CalledOnNullInput: calledOnNull,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to fetch functions: %v", err)
	}
	// Sort functions within each keyspace
	for ks := range cache.functions {
		sort.Slice(cache.functions[ks], func(i, j int) bool {
			return cache.functions[ks][i].Name < cache.functions[ks][j].Name
		})
	}

	// 7. Fetch ALL aggregates
	iter = session.Query(`SELECT keyspace_name, aggregate_name, argument_types, state_func,
		state_type, final_func, initcond FROM system_schema.aggregates`).Iter()
	var aggName, stateFunc, stateType, finalFunc, initCond string
	for iter.Scan(&ksName, &aggName, &argTypes, &stateFunc, &stateType, &finalFunc, &initCond) {
		if _, ok := cache.keyspaces[ksName]; !ok {
			continue
		}
		argTypesCopy := make([]string, len(argTypes))
		copy(argTypesCopy, argTypes)
		cache.aggregates[ksName] = append(cache.aggregates[ksName], ddlAggregateInfo{
			Name:          aggName,
			ArgumentTypes: argTypesCopy,
			StateFunc:     stateFunc,
			StateType:     stateType,
			FinalFunc:     finalFunc,
			InitCond:      initCond,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to fetch aggregates: %v", err)
	}
	// Sort aggregates within each keyspace
	for ks := range cache.aggregates {
		sort.Slice(cache.aggregates[ks], func(i, j int) bool {
			return cache.aggregates[ks][i].Name < cache.aggregates[ks][j].Name
		})
	}

	// 8. Fetch ALL views with their complete metadata
	iter = session.Query("SELECT keyspace_name, view_name, base_table_name, where_clause FROM system_schema.views").Iter()
	var viewName, baseTable, whereClause string
	for iter.Scan(&ksName, &viewName, &baseTable, &whereClause) {
		if _, ok := cache.keyspaces[ksName]; !ok {
			continue
		}
		cache.views[ksName] = append(cache.views[ksName], ddlViewInfo{
			Name:        viewName,
			BaseTable:   baseTable,
			WhereClause: whereClause,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to fetch views: %v", err)
	}
	// Sort views within each keyspace
	for ks := range cache.views {
		sort.Slice(cache.views[ks], func(i, j int) bool {
			return cache.views[ks][i].Name < cache.views[ks][j].Name
		})
	}

	return cache, nil
}

// generateKeyspaceDDLFromCache generates DDL for a keyspace using pre-fetched metadata
func generateKeyspaceDDLFromCache(cache *ddlMetadataCache, ksName string) (string, error) {
	var ddl strings.Builder

	// Get keyspace info from cache (O(1))
	ks, ok := cache.keyspaces[ksName]
	if !ok {
		return "", fmt.Errorf("keyspace %s not found", ksName)
	}

	// CREATE KEYSPACE
	ddl.WriteString(generateCreateKeyspace(ks))
	ddl.WriteString("\n\n")

	// Get and generate UDTs first (they may be referenced by tables)
	if types, ok := cache.types[ksName]; ok && len(types) > 0 {
		ddl.WriteString("-- User Defined Types\n")
		for _, t := range types {
			ddl.WriteString(generateCreateType(ksName, t))
			ddl.WriteString("\n\n")
		}
	}

	// Get and generate functions
	if functions, ok := cache.functions[ksName]; ok && len(functions) > 0 {
		ddl.WriteString("-- Functions\n")
		for _, f := range functions {
			ddl.WriteString(generateCreateFunction(ksName, f))
			ddl.WriteString("\n\n")
		}
	}

	// Get and generate aggregates
	if aggregates, ok := cache.aggregates[ksName]; ok && len(aggregates) > 0 {
		ddl.WriteString("-- Aggregates\n")
		for _, a := range aggregates {
			ddl.WriteString(generateCreateAggregate(ksName, a))
			ddl.WriteString("\n\n")
		}
	}

	// Get and generate tables with indexes
	if tables, ok := cache.tables[ksName]; ok && len(tables) > 0 {
		ddl.WriteString("-- Tables\n")
		for _, t := range tables {
			key := tableKey{keyspace: ksName, table: t.Name}
			columns := cache.columns[key]
			indexes := cache.indexes[key]

			// Generate table DDL using cached data
			ddl.WriteString(generateCreateTable(ksName, t, columns))
			ddl.WriteString("\n")

			// Generate indexes
			for _, idx := range indexes {
				ddl.WriteString(generateCreateIndex(ksName, t.Name, idx))
				ddl.WriteString("\n")
			}
		}
	}

	// Get and generate materialized views
	if views, ok := cache.views[ksName]; ok && len(views) > 0 {
		ddl.WriteString("-- Materialized Views\n")
		for _, v := range views {
			// Reconstruct view definition from cached data
			viewDef := ddlReconstructViewDefinitionFromCache(cache, ksName, v)
			ddl.WriteString(generateCreateViewWithDef(ksName, v.Name, viewDef))
			ddl.WriteString("\n\n")
		}
	}

	return ddl.String(), nil
}

// ddlReconstructViewDefinitionFromCache reconstructs view definition using cached column data
func ddlReconstructViewDefinitionFromCache(cache *ddlMetadataCache, ksName string, v ddlViewInfo) string {
	// Get view columns from cache
	key := tableKey{keyspace: ksName, table: v.Name}
	viewCols := cache.columns[key]

	var columns []string
	var pkCols []struct {
		name     string
		position int
	}
	var ckCols []struct {
		name     string
		position int
	}

	for _, col := range viewCols {
		columns = append(columns, quoteIdentifier(col.Name))
		if col.Kind == "partition_key" {
			pkCols = append(pkCols, struct {
				name     string
				position int
			}{col.Name, col.Position})
		} else if col.Kind == "clustering" {
			ckCols = append(ckCols, struct {
				name     string
				position int
			}{col.Name, col.Position})
		}
	}

	// Sort by position
	sort.Slice(pkCols, func(i, j int) bool { return pkCols[i].position < pkCols[j].position })
	sort.Slice(ckCols, func(i, j int) bool { return ckCols[i].position < ckCols[j].position })

	// Build SELECT statement
	var sb strings.Builder
	sb.WriteString("SELECT ")
	if len(columns) > 0 {
		sb.WriteString(strings.Join(columns, ", "))
	} else {
		sb.WriteString("*")
	}
	sb.WriteString(fmt.Sprintf(" FROM %s.%s", quoteIdentifier(ksName), quoteIdentifier(v.BaseTable)))

	if v.WhereClause != "" {
		sb.WriteString(fmt.Sprintf(" WHERE %s", v.WhereClause))
	}

	// Build PRIMARY KEY clause
	var pkNames []string
	for _, c := range pkCols {
		pkNames = append(pkNames, quoteIdentifier(c.name))
	}
	var ckNames []string
	for _, c := range ckCols {
		ckNames = append(ckNames, quoteIdentifier(c.name))
	}

	var pkStr string
	if len(pkNames) == 1 {
		pkStr = pkNames[0]
	} else if len(pkNames) > 1 {
		pkStr = "(" + strings.Join(pkNames, ", ") + ")"
	}

	if pkStr != "" {
		sb.WriteString(" PRIMARY KEY (")
		sb.WriteString(pkStr)
		if len(ckNames) > 0 {
			sb.WriteString(", ")
			sb.WriteString(strings.Join(ckNames, ", "))
		}
		sb.WriteString(")")
	}

	return sb.String()
}

// generateCreateViewWithDef generates CREATE MATERIALIZED VIEW with the given definition
func generateCreateViewWithDef(ksName, viewName, viewDef string) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE MATERIALIZED VIEW %s.%s AS\n",
		quoteIdentifier(ksName), quoteIdentifier(viewName)))
	sb.WriteString(fmt.Sprintf("    %s\n", viewDef))
	sb.WriteString(";")
	return sb.String()
}

// loadKeyspaceMetadata fetches all metadata for a single keyspace in batch queries
// This reduces N+1 queries to ~8 queries for the keyspace
func loadKeyspaceMetadata(session *gocql.Session, ksName string) (*ddlMetadataCache, error) {
	cache := &ddlMetadataCache{
		keyspaces:  make(map[string]ddlKeyspaceInfo),
		tables:     make(map[string][]ddlTableInfo),
		columns:    make(map[tableKey][]ddlColumnInfo),
		indexes:    make(map[tableKey][]ddlIndexInfo),
		types:      make(map[string][]ddlTypeInfo),
		functions:  make(map[string][]ddlFunctionInfo),
		aggregates: make(map[string][]ddlAggregateInfo),
		views:      make(map[string][]ddlViewInfo),
	}

	// 1. Fetch keyspace info
	var replication map[string]string
	var durableWrites bool
	err := session.Query("SELECT keyspace_name, replication, durable_writes FROM system_schema.keyspaces WHERE keyspace_name = ?", ksName).
		Scan(&ksName, &replication, &durableWrites)
	if err != nil {
		return nil, fmt.Errorf("keyspace %s not found: %v", ksName, err)
	}
	repCopy := make(map[string]string, len(replication))
	for k, v := range replication {
		repCopy[k] = v
	}
	cache.keyspaces[ksName] = ddlKeyspaceInfo{
		Name:          ksName,
		Replication:   repCopy,
		DurableWrites: durableWrites,
	}

	// 2. Fetch all tables for this keyspace
	iter := session.Query("SELECT table_name, comment FROM system_schema.tables WHERE keyspace_name = ?", ksName).Iter()
	var tableName, comment string
	for iter.Scan(&tableName, &comment) {
		cache.tables[ksName] = append(cache.tables[ksName], ddlTableInfo{
			Name:    tableName,
			Comment: comment,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to fetch tables: %v", err)
	}
	sort.Slice(cache.tables[ksName], func(i, j int) bool {
		return cache.tables[ksName][i].Name < cache.tables[ksName][j].Name
	})

	// 3. Fetch all columns for this keyspace (includes clustering_order)
	iter = session.Query(`SELECT table_name, column_name, type, kind, position, clustering_order
		FROM system_schema.columns WHERE keyspace_name = ?`, ksName).Iter()
	var colName, colType, kind, clusteringOrder string
	var position int
	clusteringCols := make(map[tableKey][]struct {
		name     string
		order    string
		position int
	})
	for iter.Scan(&tableName, &colName, &colType, &kind, &position, &clusteringOrder) {
		key := tableKey{keyspace: ksName, table: tableName}
		cache.columns[key] = append(cache.columns[key], ddlColumnInfo{
			Name:            colName,
			Type:            colType,
			Kind:            kind,
			Position:        position,
			ClusteringOrder: clusteringOrder,
		})
		if kind == "clustering" && clusteringOrder != "" && clusteringOrder != "none" {
			clusteringCols[key] = append(clusteringCols[key], struct {
				name     string
				order    string
				position int
			}{colName, clusteringOrder, position})
		}
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to fetch columns: %v", err)
	}

	// Update table info with clustering order
	for i := range cache.tables[ksName] {
		key := tableKey{keyspace: ksName, table: cache.tables[ksName][i].Name}
		if cols, ok := clusteringCols[key]; ok && len(cols) > 0 {
			sort.Slice(cols, func(a, b int) bool {
				return cols[a].position < cols[b].position
			})
			var orderParts []string
			for _, c := range cols {
				orderParts = append(orderParts, fmt.Sprintf("%s %s", quoteIdentifier(c.name), c.order))
			}
			cache.tables[ksName][i].ClusteringOrder = strings.Join(orderParts, ", ")
		}
	}

	// 4. Fetch all indexes for this keyspace
	iter = session.Query("SELECT table_name, index_name, kind, options FROM system_schema.indexes WHERE keyspace_name = ?", ksName).Iter()
	var indexName, indexKind string
	var options map[string]string
	for iter.Scan(&tableName, &indexName, &indexKind, &options) {
		key := tableKey{keyspace: ksName, table: tableName}
		optsCopy := make(map[string]string, len(options))
		for k, v := range options {
			optsCopy[k] = v
		}
		cache.indexes[key] = append(cache.indexes[key], ddlIndexInfo{
			Name:    indexName,
			Kind:    indexKind,
			Options: optsCopy,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to fetch indexes: %v", err)
	}
	for key := range cache.indexes {
		sort.Slice(cache.indexes[key], func(i, j int) bool {
			return cache.indexes[key][i].Name < cache.indexes[key][j].Name
		})
	}

	// 5. Fetch all types for this keyspace
	iter = session.Query("SELECT type_name, field_names, field_types FROM system_schema.types WHERE keyspace_name = ?", ksName).Iter()
	var typeName string
	var fields, fieldTypes []string
	for iter.Scan(&typeName, &fields, &fieldTypes) {
		fieldsCopy := make([]string, len(fields))
		copy(fieldsCopy, fields)
		typesCopy := make([]string, len(fieldTypes))
		copy(typesCopy, fieldTypes)
		cache.types[ksName] = append(cache.types[ksName], ddlTypeInfo{
			Name:   typeName,
			Fields: fieldsCopy,
			Types:  typesCopy,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to fetch types: %v", err)
	}
	sort.Slice(cache.types[ksName], func(i, j int) bool {
		return cache.types[ksName][i].Name < cache.types[ksName][j].Name
	})

	// 6. Fetch all functions for this keyspace
	iter = session.Query(`SELECT function_name, argument_names, argument_types, return_type, language, body, called_on_null_input
		FROM system_schema.functions WHERE keyspace_name = ?`, ksName).Iter()
	var funcName, returnType, language, body string
	var argNames, argTypes []string
	var calledOnNull bool
	for iter.Scan(&funcName, &argNames, &argTypes, &returnType, &language, &body, &calledOnNull) {
		argNamesCopy := make([]string, len(argNames))
		copy(argNamesCopy, argNames)
		argTypesCopy := make([]string, len(argTypes))
		copy(argTypesCopy, argTypes)
		cache.functions[ksName] = append(cache.functions[ksName], ddlFunctionInfo{
			Name:              funcName,
			ArgumentNames:     argNamesCopy,
			ArgumentTypes:     argTypesCopy,
			ReturnType:        returnType,
			Language:          language,
			Body:              body,
			CalledOnNullInput: calledOnNull,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to fetch functions: %v", err)
	}
	sort.Slice(cache.functions[ksName], func(i, j int) bool {
		return cache.functions[ksName][i].Name < cache.functions[ksName][j].Name
	})

	// 7. Fetch all aggregates for this keyspace
	iter = session.Query(`SELECT aggregate_name, argument_types, state_func, state_type, final_func, initcond
		FROM system_schema.aggregates WHERE keyspace_name = ?`, ksName).Iter()
	var aggName, stateFunc, stateType, finalFunc, initCond string
	for iter.Scan(&aggName, &argTypes, &stateFunc, &stateType, &finalFunc, &initCond) {
		argTypesCopy := make([]string, len(argTypes))
		copy(argTypesCopy, argTypes)
		cache.aggregates[ksName] = append(cache.aggregates[ksName], ddlAggregateInfo{
			Name:          aggName,
			ArgumentTypes: argTypesCopy,
			StateFunc:     stateFunc,
			StateType:     stateType,
			FinalFunc:     finalFunc,
			InitCond:      initCond,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to fetch aggregates: %v", err)
	}
	sort.Slice(cache.aggregates[ksName], func(i, j int) bool {
		return cache.aggregates[ksName][i].Name < cache.aggregates[ksName][j].Name
	})

	// 8. Fetch all views for this keyspace
	iter = session.Query("SELECT view_name, base_table_name, where_clause FROM system_schema.views WHERE keyspace_name = ?", ksName).Iter()
	var viewName, baseTable, whereClause string
	for iter.Scan(&viewName, &baseTable, &whereClause) {
		cache.views[ksName] = append(cache.views[ksName], ddlViewInfo{
			Name:        viewName,
			BaseTable:   baseTable,
			WhereClause: whereClause,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to fetch views: %v", err)
	}
	sort.Slice(cache.views[ksName], func(i, j int) bool {
		return cache.views[ksName][i].Name < cache.views[ksName][j].Name
	})

	return cache, nil
}

// loadTableMetadata fetches metadata for a single table in batch queries
// This reduces queries from 4 (table + clustering + columns + indexes) to 3
func loadTableMetadata(session *gocql.Session, ksName, tableName string) (ddlTableInfo, []ddlColumnInfo, []ddlIndexInfo, error) {
	var table ddlTableInfo

	// 1. Fetch table info
	err := session.Query("SELECT table_name, comment FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?", ksName, tableName).
		Scan(&table.Name, &table.Comment)
	if err != nil {
		return table, nil, nil, fmt.Errorf("table %s.%s not found: %v", ksName, tableName, err)
	}

	// 2. Fetch columns (includes clustering_order - no separate query needed)
	iter := session.Query(`SELECT column_name, type, kind, position, clustering_order
		FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?`, ksName, tableName).Iter()
	var columns []ddlColumnInfo
	var clusteringCols []struct {
		name     string
		order    string
		position int
	}
	var colName, colType, kind, clusteringOrder string
	var position int
	for iter.Scan(&colName, &colType, &kind, &position, &clusteringOrder) {
		columns = append(columns, ddlColumnInfo{
			Name:            colName,
			Type:            colType,
			Kind:            kind,
			Position:        position,
			ClusteringOrder: clusteringOrder,
		})
		if kind == "clustering" && clusteringOrder != "" && clusteringOrder != "none" {
			clusteringCols = append(clusteringCols, struct {
				name     string
				order    string
				position int
			}{colName, clusteringOrder, position})
		}
	}
	if err := iter.Close(); err != nil {
		return table, nil, nil, fmt.Errorf("failed to fetch columns: %v", err)
	}

	// Build clustering order from columns
	if len(clusteringCols) > 0 {
		sort.Slice(clusteringCols, func(i, j int) bool {
			return clusteringCols[i].position < clusteringCols[j].position
		})
		var orderParts []string
		for _, c := range clusteringCols {
			orderParts = append(orderParts, fmt.Sprintf("%s %s", quoteIdentifier(c.name), c.order))
		}
		table.ClusteringOrder = strings.Join(orderParts, ", ")
	}

	// 3. Fetch indexes
	iter = session.Query("SELECT index_name, kind, options FROM system_schema.indexes WHERE keyspace_name = ? AND table_name = ?", ksName, tableName).Iter()
	var indexes []ddlIndexInfo
	var indexName, indexKind string
	var options map[string]string
	for iter.Scan(&indexName, &indexKind, &options) {
		optsCopy := make(map[string]string, len(options))
		for k, v := range options {
			optsCopy[k] = v
		}
		indexes = append(indexes, ddlIndexInfo{
			Name:    indexName,
			Kind:    indexKind,
			Options: optsCopy,
		})
	}
	if err := iter.Close(); err != nil {
		return table, nil, nil, fmt.Errorf("failed to fetch indexes: %v", err)
	}
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i].Name < indexes[j].Name
	})

	return table, columns, indexes, nil
}

func generateClusterDDL(session *gocql.Session, includeSystem bool) (*DDLResult, error) {
	// Load all metadata in batch (8-10 queries total)
	cache, err := loadAllMetadata(session, includeSystem)
	if err != nil {
		return nil, err
	}

	// Collect and sort keyspace names
	var keyspaceNames []string
	for name := range cache.keyspaces {
		keyspaceNames = append(keyspaceNames, name)
	}
	sort.Strings(keyspaceNames)

	// Process keyspaces concurrently
	type result struct {
		name string
		ddl  string
		err  error
	}

	results := make(chan result, len(keyspaceNames))
	var wg sync.WaitGroup

	for _, ksName := range keyspaceNames {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			ddl, err := generateKeyspaceDDLFromCache(cache, name)
			results <- result{name: name, ddl: ddl, err: err}
		}(ksName)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	ddlMap := make(map[string]string)
	for r := range results {
		if r.err != nil {
			return nil, r.err
		}
		ddlMap[r.name] = r.ddl
	}

	// Build final DDL in sorted order
	var ddl strings.Builder
	for i, name := range keyspaceNames {
		if i > 0 {
			ddl.WriteString("\n")
		}
		ddl.WriteString(ddlMap[name])
	}

	return &DDLResult{
		DDL:   ddl.String(),
		Scope: "cluster",
	}, nil
}

func generateKeyspaceDDL(session *gocql.Session, ksName string) (*DDLResult, error) {
	// Load all keyspace metadata in batch (8 queries total)
	cache, err := loadKeyspaceMetadata(session, ksName)
	if err != nil {
		return nil, err
	}

	// Use the cached generator
	ddlStr, err := generateKeyspaceDDLFromCache(cache, ksName)
	if err != nil {
		return nil, err
	}

	return &DDLResult{
		DDL:   strings.TrimSpace(ddlStr),
		Scope: fmt.Sprintf("keyspace>%s", ksName),
	}, nil
}

func generateTableDDL(session *gocql.Session, ksName, tableName string) (*DDLResult, error) {
	ddl, err := generateFullTableDDL(session, ksName, tableName)
	if err != nil {
		return nil, err
	}

	return &DDLResult{
		DDL:   strings.TrimSpace(ddl),
		Scope: fmt.Sprintf("keyspace>%s>table>%s", ksName, tableName),
	}, nil
}

func generateFullTableDDL(session *gocql.Session, ksName, tableName string) (string, error) {
	// Load table metadata in batch (3 queries instead of 4)
	table, columns, indexes, err := loadTableMetadata(session, ksName, tableName)
	if err != nil {
		return "", err
	}

	var ddl strings.Builder
	ddl.WriteString(generateCreateTable(ksName, table, columns))
	ddl.WriteString("\n")

	// Add indexes
	for _, idx := range indexes {
		ddl.WriteString(generateCreateIndex(ksName, tableName, idx))
		ddl.WriteString("\n")
	}

	return ddl.String(), nil
}

func generateIndexDDL(session *gocql.Session, ksName, tableName, indexName string) (*DDLResult, error) {
	indexes, err := ddlGetIndexes(session, ksName, tableName)
	if err != nil {
		return nil, err
	}

	for _, idx := range indexes {
		if idx.Name == indexName {
			return &DDLResult{
				DDL:   strings.TrimSpace(generateCreateIndex(ksName, tableName, idx)),
				Scope: fmt.Sprintf("keyspace>%s>table>%s>index>%s", ksName, tableName, indexName),
			}, nil
		}
	}

	return nil, fmt.Errorf("index %s not found on table %s.%s", indexName, ksName, tableName)
}

func generateTypeDDL(session *gocql.Session, ksName, typeName string) (*DDLResult, error) {
	types, err := ddlGetTypes(session, ksName)
	if err != nil {
		return nil, err
	}

	for _, t := range types {
		if t.Name == typeName {
			return &DDLResult{
				DDL:   strings.TrimSpace(generateCreateType(ksName, t)),
				Scope: fmt.Sprintf("keyspace>%s>type>%s", ksName, typeName),
			}, nil
		}
	}

	return nil, fmt.Errorf("type %s not found in keyspace %s", typeName, ksName)
}

func generateFunctionDDL(session *gocql.Session, ksName, funcName string) (*DDLResult, error) {
	functions, err := ddlGetFunctions(session, ksName)
	if err != nil {
		return nil, err
	}

	for _, f := range functions {
		if f.Name == funcName {
			return &DDLResult{
				DDL:   strings.TrimSpace(generateCreateFunction(ksName, f)),
				Scope: fmt.Sprintf("keyspace>%s>function>%s", ksName, funcName),
			}, nil
		}
	}

	return nil, fmt.Errorf("function %s not found in keyspace %s", funcName, ksName)
}

func generateAggregateDDL(session *gocql.Session, ksName, aggName string) (*DDLResult, error) {
	aggregates, err := ddlGetAggregates(session, ksName)
	if err != nil {
		return nil, err
	}

	for _, a := range aggregates {
		if a.Name == aggName {
			return &DDLResult{
				DDL:   strings.TrimSpace(generateCreateAggregate(ksName, a)),
				Scope: fmt.Sprintf("keyspace>%s>aggregate>%s", ksName, aggName),
			}, nil
		}
	}

	return nil, fmt.Errorf("aggregate %s not found in keyspace %s", aggName, ksName)
}

func generateViewDDL(session *gocql.Session, ksName, viewName string) (*DDLResult, error) {
	views, err := ddlGetViews(session, ksName)
	if err != nil {
		return nil, err
	}

	for _, v := range views {
		if v.Name == viewName {
			return &DDLResult{
				DDL:   strings.TrimSpace(generateCreateView(ksName, v)),
				Scope: fmt.Sprintf("keyspace>%s>view>%s", ksName, viewName),
			}, nil
		}
	}

	return nil, fmt.Errorf("materialized view %s not found in keyspace %s", viewName, ksName)
}

// Helper functions to generate CREATE statements

// ddlKeyspaceInfo represents keyspace info for DDL generation
type ddlKeyspaceInfo struct {
	Name          string
	Replication   map[string]string
	DurableWrites bool
	IsVirtual     bool
}

// ddlTableInfo represents table info for DDL generation
type ddlTableInfo struct {
	Name            string
	Comment         string
	ClusteringOrder string
	IsVirtual       bool
}

// ddlTypeInfo represents user type info for DDL generation
type ddlTypeInfo struct {
	Name   string
	Fields []string
	Types  []string
}

// ddlFunctionInfo represents function info for DDL generation
type ddlFunctionInfo struct {
	Name              string
	ArgumentNames     []string
	ArgumentTypes     []string
	ReturnType        string
	Language          string
	Body              string
	CalledOnNullInput bool
}

// ddlAggregateInfo represents aggregate info for DDL generation
type ddlAggregateInfo struct {
	Name          string
	ArgumentTypes []string
	StateFunc     string
	StateType     string
	FinalFunc     string
	InitCond      string
}

// ddlViewInfo represents view info for DDL generation
type ddlViewInfo struct {
	Name           string
	BaseTable      string
	ViewDefinition string
	WhereClause    string
}

// ddlIndexInfo represents index info for DDL generation
type ddlIndexInfo struct {
	Name    string
	Kind    string
	Options map[string]string
}

// ddlColumnInfo represents column info for DDL generation
type ddlColumnInfo struct {
	Name            string
	Type            string
	Kind            string
	Position        int
	ClusteringOrder string
}

func generateCreateKeyspace(ks ddlKeyspaceInfo) string {
	var sb strings.Builder

	// Virtual keyspaces cannot be created with DDL
	if ks.IsVirtual {
		sb.WriteString(fmt.Sprintf("-- Virtual Keyspace: %s (read-only, cannot be created with DDL)", ks.Name))
		return sb.String()
	}

	sb.WriteString(fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {", quoteIdentifier(ks.Name)))

	// Build replication map
	var repParts []string
	for k, v := range ks.Replication {
		repParts = append(repParts, fmt.Sprintf("'%s': '%s'", k, v))
	}
	sort.Strings(repParts)
	sb.WriteString(strings.Join(repParts, ", "))

	sb.WriteString("}")

	if !ks.DurableWrites {
		sb.WriteString(" AND durable_writes = false")
	}

	sb.WriteString(";")

	return sb.String()
}

func generateCreateType(ksName string, t ddlTypeInfo) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE TYPE %s.%s (\n", quoteIdentifier(ksName), quoteIdentifier(t.Name)))

	for i, field := range t.Fields {
		sb.WriteString(fmt.Sprintf("    %s %s", quoteIdentifier(field), t.Types[i]))
		if i < len(t.Fields)-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}

	sb.WriteString(");")

	return sb.String()
}

func generateCreateTable(ksName string, table ddlTableInfo, columns []ddlColumnInfo) string {
	var sb strings.Builder

	// Virtual tables cannot be created with DDL - output as comment with schema info
	if table.IsVirtual {
		sb.WriteString(fmt.Sprintf("-- Virtual Table: %s.%s (read-only, cannot be created with DDL)\n", ksName, table.Name))
		sb.WriteString(fmt.Sprintf("-- Columns: "))
		var colDefs []string
		for _, col := range columns {
			colDefs = append(colDefs, fmt.Sprintf("%s %s", col.Name, col.Type))
		}
		sb.WriteString(strings.Join(colDefs, ", "))
		return sb.String()
	}

	sb.WriteString(fmt.Sprintf("CREATE TABLE %s.%s (\n", quoteIdentifier(ksName), quoteIdentifier(table.Name)))

	// Sort columns: partition key first, then clustering, then regular
	sortedColumns := make([]ddlColumnInfo, len(columns))
	copy(sortedColumns, columns)
	sort.Slice(sortedColumns, func(i, j int) bool {
		kindOrder := map[string]int{"partition_key": 0, "clustering": 1, "static": 2, "regular": 3}
		if kindOrder[sortedColumns[i].Kind] != kindOrder[sortedColumns[j].Kind] {
			return kindOrder[sortedColumns[i].Kind] < kindOrder[sortedColumns[j].Kind]
		}
		return sortedColumns[i].Position < sortedColumns[j].Position
	})

	// Write column definitions
	for i, col := range sortedColumns {
		sb.WriteString(fmt.Sprintf("    %s %s", quoteIdentifier(col.Name), col.Type))
		if col.Kind == "static" {
			sb.WriteString(" STATIC")
		}
		if i < len(sortedColumns)-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}

	// Build PRIMARY KEY
	var partitionKey []string
	var clusteringKey []string

	// Collect and sort by position
	type keyCol struct {
		name     string
		position int
	}
	var pkCols, ckCols []keyCol

	for _, col := range sortedColumns {
		if col.Kind == "partition_key" {
			pkCols = append(pkCols, keyCol{col.Name, col.Position})
		} else if col.Kind == "clustering" {
			ckCols = append(ckCols, keyCol{col.Name, col.Position})
		}
	}

	sort.Slice(pkCols, func(i, j int) bool { return pkCols[i].position < pkCols[j].position })
	sort.Slice(ckCols, func(i, j int) bool { return ckCols[i].position < ckCols[j].position })

	for _, c := range pkCols {
		partitionKey = append(partitionKey, quoteIdentifier(c.name))
	}
	for _, c := range ckCols {
		clusteringKey = append(clusteringKey, quoteIdentifier(c.name))
	}

	var pkStr string
	if len(partitionKey) == 1 {
		pkStr = partitionKey[0]
	} else if len(partitionKey) > 1 {
		pkStr = "(" + strings.Join(partitionKey, ", ") + ")"
	}

	if pkStr != "" {
		if len(clusteringKey) > 0 {
			sb.WriteString(fmt.Sprintf("    PRIMARY KEY (%s, %s)\n", pkStr, strings.Join(clusteringKey, ", ")))
		} else {
			sb.WriteString(fmt.Sprintf("    PRIMARY KEY (%s)\n", pkStr))
		}
	}

	sb.WriteString(")")

	// Add table options
	var options []string

	if table.ClusteringOrder != "" {
		options = append(options, fmt.Sprintf("CLUSTERING ORDER BY (%s)", table.ClusteringOrder))
	}

	if table.Comment != "" {
		options = append(options, fmt.Sprintf("comment = '%s'", escapeString(table.Comment)))
	}

	if len(options) > 0 {
		sb.WriteString(" WITH ")
		sb.WriteString(strings.Join(options, " AND "))
	}

	sb.WriteString(";")

	return sb.String()
}

func generateCreateIndex(ksName, tableName string, idx ddlIndexInfo) string {
	var sb strings.Builder

	sb.WriteString("CREATE")
	if idx.Kind == "CUSTOM" {
		sb.WriteString(" CUSTOM")
	}
	sb.WriteString(fmt.Sprintf(" INDEX %s ON %s.%s ",
		quoteIdentifier(idx.Name),
		quoteIdentifier(ksName),
		quoteIdentifier(tableName)))

	// Get target from options
	target := idx.Options["target"]
	if target != "" {
		sb.WriteString(fmt.Sprintf("(%s)", target))
	}

	if idx.Kind == "CUSTOM" {
		if className, ok := idx.Options["class_name"]; ok {
			sb.WriteString(fmt.Sprintf(" USING '%s'", className))
		}
	}

	sb.WriteString(";")

	return sb.String()
}

func generateCreateFunction(ksName string, f ddlFunctionInfo) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE FUNCTION %s.%s(", quoteIdentifier(ksName), quoteIdentifier(f.Name)))

	// Arguments
	var args []string
	for i, argName := range f.ArgumentNames {
		argType := ""
		if i < len(f.ArgumentTypes) {
			argType = f.ArgumentTypes[i]
		}
		args = append(args, fmt.Sprintf("%s %s", argName, argType))
	}
	sb.WriteString(strings.Join(args, ", "))

	sb.WriteString(")")

	// Return type and null handling
	if f.CalledOnNullInput {
		sb.WriteString(" CALLED ON NULL INPUT")
	} else {
		sb.WriteString(" RETURNS NULL ON NULL INPUT")
	}

	sb.WriteString(fmt.Sprintf(" RETURNS %s", f.ReturnType))
	sb.WriteString(fmt.Sprintf(" LANGUAGE %s", f.Language))
	sb.WriteString(fmt.Sprintf(" AS $$%s$$", f.Body))
	sb.WriteString(";")

	return sb.String()
}

func generateCreateAggregate(ksName string, a ddlAggregateInfo) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE AGGREGATE %s.%s(", quoteIdentifier(ksName), quoteIdentifier(a.Name)))
	sb.WriteString(strings.Join(a.ArgumentTypes, ", "))
	sb.WriteString(")")

	sb.WriteString(fmt.Sprintf(" SFUNC %s", a.StateFunc))
	sb.WriteString(fmt.Sprintf(" STYPE %s", a.StateType))

	if a.FinalFunc != "" {
		sb.WriteString(fmt.Sprintf(" FINALFUNC %s", a.FinalFunc))
	}

	if a.InitCond != "" {
		sb.WriteString(fmt.Sprintf(" INITCOND %s", a.InitCond))
	}

	sb.WriteString(";")

	return sb.String()
}

func generateCreateView(ksName string, v ddlViewInfo) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE MATERIALIZED VIEW %s.%s AS\n",
		quoteIdentifier(ksName), quoteIdentifier(v.Name)))

	sb.WriteString(fmt.Sprintf("    %s\n", v.ViewDefinition))

	sb.WriteString(";")

	return sb.String()
}

// Query helper functions (prefixed with ddl to avoid conflicts with metadata.go)

func ddlGetKeyspaces(session *gocql.Session) ([]ddlKeyspaceInfo, error) {
	var keyspaces []ddlKeyspaceInfo

	iter := session.Query("SELECT keyspace_name, replication, durable_writes FROM system_schema.keyspaces").Iter()
	var name string
	var replication map[string]string
	var durableWrites bool

	for iter.Scan(&name, &replication, &durableWrites) {
		keyspaces = append(keyspaces, ddlKeyspaceInfo{
			Name:          name,
			Replication:   replication,
			DurableWrites: durableWrites,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	sort.Slice(keyspaces, func(i, j int) bool {
		return keyspaces[i].Name < keyspaces[j].Name
	})

	return keyspaces, nil
}

func ddlGetKeyspaceInfo(session *gocql.Session, ksName string) (ddlKeyspaceInfo, error) {
	var ks ddlKeyspaceInfo

	var replication map[string]string
	var durableWrites bool

	err := session.Query("SELECT keyspace_name, replication, durable_writes FROM system_schema.keyspaces WHERE keyspace_name = ?", ksName).
		Scan(&ks.Name, &replication, &durableWrites)

	if err != nil {
		return ks, fmt.Errorf("keyspace %s not found: %v", ksName, err)
	}

	ks.Replication = replication
	ks.DurableWrites = durableWrites

	return ks, nil
}

func ddlGetTables(session *gocql.Session, ksName string) ([]ddlTableInfo, error) {
	var tables []ddlTableInfo

	iter := session.Query("SELECT table_name, comment FROM system_schema.tables WHERE keyspace_name = ?", ksName).Iter()
	var name, comment string

	for iter.Scan(&name, &comment) {
		tables = append(tables, ddlTableInfo{
			Name:    name,
			Comment: comment,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Name < tables[j].Name
	})

	return tables, nil
}

func ddlGetTableInfo(session *gocql.Session, ksName, tableName string) (ddlTableInfo, error) {
	var table ddlTableInfo

	err := session.Query("SELECT table_name, comment FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?", ksName, tableName).
		Scan(&table.Name, &table.Comment)

	if err != nil {
		return table, fmt.Errorf("table %s.%s not found: %v", ksName, tableName, err)
	}

	// Get clustering order
	var clusteringOrders []string
	iter := session.Query(`
		SELECT column_name, clustering_order
		FROM system_schema.columns
		WHERE keyspace_name = ? AND table_name = ? AND kind = 'clustering'
		ALLOW FILTERING`, ksName, tableName).Iter()

	var colName, order string
	for iter.Scan(&colName, &order) {
		if order != "" && order != "none" {
			clusteringOrders = append(clusteringOrders, fmt.Sprintf("%s %s", quoteIdentifier(colName), order))
		}
	}
	iter.Close()

	if len(clusteringOrders) > 0 {
		table.ClusteringOrder = strings.Join(clusteringOrders, ", ")
	}

	return table, nil
}

func ddlGetColumns(session *gocql.Session, ksName, tableName string) ([]ddlColumnInfo, error) {
	var columns []ddlColumnInfo

	iter := session.Query(`
		SELECT column_name, type, kind, position
		FROM system_schema.columns
		WHERE keyspace_name = ? AND table_name = ?`, ksName, tableName).Iter()

	var name, colType, kind string
	var position int

	for iter.Scan(&name, &colType, &kind, &position) {
		columns = append(columns, ddlColumnInfo{
			Name:     name,
			Type:     colType,
			Kind:     kind,
			Position: position,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	return columns, nil
}

func ddlGetIndexes(session *gocql.Session, ksName, tableName string) ([]ddlIndexInfo, error) {
	var indexes []ddlIndexInfo

	iter := session.Query(`
		SELECT index_name, kind, options
		FROM system_schema.indexes
		WHERE keyspace_name = ? AND table_name = ?`, ksName, tableName).Iter()

	var name, kind string
	var options map[string]string

	for iter.Scan(&name, &kind, &options) {
		indexes = append(indexes, ddlIndexInfo{
			Name:    name,
			Kind:    kind,
			Options: options,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i].Name < indexes[j].Name
	})

	return indexes, nil
}

func ddlGetTypes(session *gocql.Session, ksName string) ([]ddlTypeInfo, error) {
	var types []ddlTypeInfo

	iter := session.Query(`
		SELECT type_name, field_names, field_types
		FROM system_schema.types
		WHERE keyspace_name = ?`, ksName).Iter()

	var name string
	var fields, fieldTypes []string

	for iter.Scan(&name, &fields, &fieldTypes) {
		types = append(types, ddlTypeInfo{
			Name:   name,
			Fields: fields,
			Types:  fieldTypes,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	sort.Slice(types, func(i, j int) bool {
		return types[i].Name < types[j].Name
	})

	return types, nil
}

func ddlGetFunctions(session *gocql.Session, ksName string) ([]ddlFunctionInfo, error) {
	var functions []ddlFunctionInfo

	iter := session.Query(`
		SELECT function_name, argument_names, argument_types, return_type, language, body, called_on_null_input
		FROM system_schema.functions
		WHERE keyspace_name = ?`, ksName).Iter()

	var name, returnType, language, body string
	var argNames, argTypes []string
	var calledOnNull bool

	for iter.Scan(&name, &argNames, &argTypes, &returnType, &language, &body, &calledOnNull) {
		functions = append(functions, ddlFunctionInfo{
			Name:              name,
			ArgumentNames:     argNames,
			ArgumentTypes:     argTypes,
			ReturnType:        returnType,
			Language:          language,
			Body:              body,
			CalledOnNullInput: calledOnNull,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	sort.Slice(functions, func(i, j int) bool {
		return functions[i].Name < functions[j].Name
	})

	return functions, nil
}

func ddlGetAggregates(session *gocql.Session, ksName string) ([]ddlAggregateInfo, error) {
	var aggregates []ddlAggregateInfo

	iter := session.Query(`
		SELECT aggregate_name, argument_types, state_func, state_type, final_func, initcond
		FROM system_schema.aggregates
		WHERE keyspace_name = ?`, ksName).Iter()

	var name, stateFunc, stateType, finalFunc, initCond string
	var argTypes []string

	for iter.Scan(&name, &argTypes, &stateFunc, &stateType, &finalFunc, &initCond) {
		aggregates = append(aggregates, ddlAggregateInfo{
			Name:          name,
			ArgumentTypes: argTypes,
			StateFunc:     stateFunc,
			StateType:     stateType,
			FinalFunc:     finalFunc,
			InitCond:      initCond,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	sort.Slice(aggregates, func(i, j int) bool {
		return aggregates[i].Name < aggregates[j].Name
	})

	return aggregates, nil
}

func ddlGetViews(session *gocql.Session, ksName string) ([]ddlViewInfo, error) {
	var views []ddlViewInfo

	iter := session.Query(`
		SELECT view_name, base_table_name, where_clause
		FROM system_schema.views
		WHERE keyspace_name = ?`, ksName).Iter()

	var name, baseTable, whereClause string

	for iter.Scan(&name, &baseTable, &whereClause) {
		// Reconstruct view definition from schema
		viewDef := ddlReconstructViewDefinition(session, ksName, name, baseTable, whereClause)
		views = append(views, ddlViewInfo{
			Name:           name,
			BaseTable:      baseTable,
			ViewDefinition: viewDef,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	sort.Slice(views, func(i, j int) bool {
		return views[i].Name < views[j].Name
	})

	return views, nil
}

func ddlReconstructViewDefinition(session *gocql.Session, ksName, viewName, baseTable, whereClause string) string {
	// Get view columns
	var columns []string
	iter := session.Query(`
		SELECT column_name FROM system_schema.columns
		WHERE keyspace_name = ? AND table_name = ?`, ksName, viewName).Iter()

	var colName string
	for iter.Scan(&colName) {
		columns = append(columns, quoteIdentifier(colName))
	}
	iter.Close()

	// Build SELECT statement
	var sb strings.Builder
	sb.WriteString("SELECT ")
	if len(columns) > 0 {
		sb.WriteString(strings.Join(columns, ", "))
	} else {
		sb.WriteString("*")
	}
	sb.WriteString(fmt.Sprintf(" FROM %s.%s", quoteIdentifier(ksName), quoteIdentifier(baseTable)))

	if whereClause != "" {
		sb.WriteString(fmt.Sprintf(" WHERE %s", whereClause))
	}

	// Get primary key for view
	var pkCols []string
	var ckCols []string

	iter = session.Query(`
		SELECT column_name, kind, position FROM system_schema.columns
		WHERE keyspace_name = ? AND table_name = ? AND kind IN ('partition_key', 'clustering')
		ALLOW FILTERING`, ksName, viewName).Iter()

	var kind string
	var position int
	for iter.Scan(&colName, &kind, &position) {
		if kind == "partition_key" {
			pkCols = append(pkCols, quoteIdentifier(colName))
		} else if kind == "clustering" {
			ckCols = append(ckCols, quoteIdentifier(colName))
		}
	}
	iter.Close()

	// Build PRIMARY KEY clause
	var pkStr string
	if len(pkCols) == 1 {
		pkStr = pkCols[0]
	} else if len(pkCols) > 1 {
		pkStr = "(" + strings.Join(pkCols, ", ") + ")"
	}

	if pkStr != "" {
		sb.WriteString(" PRIMARY KEY (")
		sb.WriteString(pkStr)
		if len(ckCols) > 0 {
			sb.WriteString(", ")
			sb.WriteString(strings.Join(ckCols, ", "))
		}
		sb.WriteString(")")
	}

	return sb.String()
}

// Utility functions

func quoteIdentifier(name string) string {
	// Check if identifier needs quoting
	needsQuoting := false

	// Reserved words (simplified list)
	reserved := map[string]bool{
		"add": true, "allow": true, "alter": true, "and": true, "any": true,
		"apply": true, "asc": true, "authorize": true, "batch": true, "begin": true,
		"by": true, "columnfamily": true, "create": true, "delete": true, "desc": true,
		"drop": true, "each_quorum": true, "from": true, "grant": true, "in": true,
		"index": true, "inet": true, "infinity": true, "insert": true, "into": true,
		"key": true, "keyspace": true, "keyspaces": true, "limit": true, "local_one": true,
		"local_quorum": true, "modify": true, "nan": true, "norecursive": true, "not": true,
		"of": true, "on": true, "one": true, "order": true, "password": true,
		"primary": true, "quorum": true, "rename": true, "revoke": true, "schema": true,
		"select": true, "set": true, "table": true, "three": true, "to": true,
		"token": true, "truncate": true, "two": true, "unlogged": true, "update": true,
		"use": true, "using": true, "where": true, "with": true,
	}

	lower := strings.ToLower(name)
	if reserved[lower] {
		needsQuoting = true
	}

	// Check for special characters
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
			needsQuoting = true
			break
		}
	}

	// Check if starts with number
	if len(name) > 0 && name[0] >= '0' && name[0] <= '9' {
		needsQuoting = true
	}

	// Check for uppercase (CQL identifiers are case-insensitive unless quoted)
	for _, c := range name {
		if c >= 'A' && c <= 'Z' {
			needsQuoting = true
			break
		}
	}

	if needsQuoting {
		return fmt.Sprintf("\"%s\"", strings.ReplaceAll(name, "\"", "\"\""))
	}

	return name
}

func escapeString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func isSystemKeyspace(name string) bool {
	systemKeyspaces := map[string]bool{
		"system":                true,
		"system_schema":         true,
		"system_auth":           true,
		"system_distributed":    true,
		"system_traces":         true,
		"system_views":          true,
		"system_virtual_schema": true,
	}
	return systemKeyspaces[name]
}
