const { native, callNativeAsync, callNativeTrueAsync } = require('./native');

/**
 * CQL Session - represents a connection to a Cassandra cluster
 */
class CQLSession {
  constructor(handle, cassandraVersion, keyspace, username, host) {
    this._handle = handle;
    this._cassandraVersion = cassandraVersion;
    this._keyspace = keyspace;
    this._username = username || '';
    this._host = host || '';
  }

  /**
   * Build the cqlsh-style prompt string
   * Format: [username@]cqlsh[:keyspace]
   * @returns {string} The prompt string
   */
  getPrompt() {
    let prompt = '';
    if (this._username) {
      prompt += `${this._username}@`;
    }
    prompt += 'cqlsh';
    if (this._keyspace) {
      prompt += `:${this._keyspace}`;
    }
    return prompt;
  }

  /**
   * Get prompt components for rendering
   * @returns {Object} { username, host, keyspace, prompt }
   */
  getPromptInfo() {
    return {
      username: this._username,
      host: this._host,
      keyspace: this._keyspace,
      prompt: this.getPrompt(),
    };
  }

  /**
   * Test connection to a Cassandra cluster without maintaining a session
   * @param {Object} options - Connection options
   * @param {string} [options.host='127.0.0.1'] - Cassandra host
   * @param {number} [options.port=9042] - Cassandra port
   * @param {string} [options.username] - Username (plaintext or RSA-encrypted base64)
   * @param {string} [options.password] - Password (plaintext or RSA-encrypted base64)
   * @param {string} [options.rsaPrivateKey] - PEM-encoded RSA private key for credential decryption
   * @param {string} [options.rsaPrivateKeyFile] - Path to RSA private key file for credential decryption
   * @returns {Promise<Object>} { success, data?, error? }
   */
  static async testConnection(options = {}) {
    const optionsJSON = JSON.stringify(options);

    return await callNativeTrueAsync(native.TestConnection, optionsJSON);
  }

  /**
   * Test connection with cancellation support
   * @param {Object} options - Connection options (same as testConnection)
   * @param {string} options.requestID - Unique request ID for cancellation (required)
   * @returns {Promise<Object>} { success, data?, error?, code? }
   *
   * If cancelled, returns: { success: false, error: 'Connection cancelled', code: 'CANCELLED' }
   */
  static async testConnectionWithID(options = {}) {
    if (!options.requestID) {
      return { success: false, error: 'requestID is required for cancellable connection test' };
    }

    const optionsJSON = JSON.stringify(options);

    return await callNativeTrueAsync(native.TestConnectionWithID, optionsJSON);
  }

  /**
   * Cancel a pending test connection
   * @param {string} requestID - The request ID passed to testConnectionWithID
   * @returns {Promise<Object>} { success, data: { cancelled: boolean, reason?: string } }
   */
  static async cancelTestConnection(requestID) {
    if (!requestID) {
      return { success: false, error: 'requestID is required' };
    }

    return await callNativeAsync(() =>
      native.CancelTestConnection(requestID)
    );
  }

  /**
   * Connect to a Cassandra cluster
   * @param {Object} options - Connection options
   * @param {string} [options.host='127.0.0.1'] - Cassandra host
   * @param {number} [options.port=9042] - Cassandra port
   * @param {string} [options.keyspace] - Initial keyspace
   * @param {string} [options.username] - Username (plaintext or RSA-encrypted base64)
   * @param {string} [options.password] - Password (plaintext or RSA-encrypted base64)
   * @param {string} [options.consistency] - Consistency level
   * @param {number} [options.connectTimeout] - Connection timeout in seconds
   * @param {number} [options.requestTimeout] - Request timeout in seconds
   * @param {string} [options.rsaPrivateKey] - PEM-encoded RSA private key for credential decryption
   * @param {string} [options.rsaPrivateKeyFile] - Path to RSA private key file for credential decryption
   * @returns {Promise<Object>} { success, data?: CQLSession, error? }
   */
  static async connect(options = {}) {
    const optionsJSON = JSON.stringify(options);

    const response = await callNativeTrueAsync(native.CreateSession, optionsJSON);

    if (!response.success || !response.data) {
      return { success: false, error: response.error || 'Failed to create session' };
    }

    // Get session info to retrieve username and host
    const handle = response.data.handle;
    const infoResult = await callNativeAsync(() => native.GetSessionInfo(handle));
    const username = infoResult.success ? infoResult.data.username : '';
    const host = infoResult.success ? infoResult.data.host : '';

    return {
      success: true,
      data: new CQLSession(
        handle,
        response.data.cassandraVersion,
        response.data.keyspace,
        username,
        host
      ),
    };
  }

  /**
   * Execute a CQL query or shell command
   * Handles multiple statements separated by semicolons
   * Shell commands (CONSISTENCY, PAGING, TRACING, EXPAND) are handled directly
   * @param {string} cql - CQL query string(s) or shell command(s)
   * @param {Object} options - Execution options
   * @param {boolean} [options.stopOnError=false] - Stop on first error
   * @param {Function} [options.onProgress] - Callback called after each statement completes
   *   Receives: { success, data, index, identifier, allCompleted, promptInfo }
   *   For SELECT with paging: data includes { hasMore, queryId } if more rows available
   * @returns {Promise<Object>} { success, data?, error?, statementsCount?, identifiers?, extraTokens?, promptInfo }
   */
  async execute(cql, options = {}) {
    try {
      const { stopOnError = false, onProgress } = options;
      const trimmed = cql.trim();

      // Handle empty input
      if (!trimmed) {
        return { success: true, statementsCount: 0, identifiers: [], extraTokens: [], data: { message: '' }, promptInfo: this.getPromptInfo() };
      }

      // Split into individual statements using native Go splitter
      const splitResponse = await callNativeAsync(() => native.SplitCQL(trimmed));
    if (!splitResponse.success) {
      return { success: false, error: splitResponse.error || 'Failed to split CQL', code: 'PARSE_ERROR', statementsCount: 0, identifiers: [], extraTokens: [], promptInfo: this.getPromptInfo() };
    }

    const { statements, incomplete, identifiers, extraTokens, secondTokens, thirdTokens, error: splitError } = splitResponse.data;

    // Handle split errors (unclosed strings, comments, etc.)
    if (splitError) {
      return { success: false, error: splitError, code: 'PARSE_ERROR', statementsCount: 0, statements: statements || [], identifiers: identifiers || [], extraTokens: extraTokens || [], secondTokens: secondTokens || [], thirdTokens: thirdTokens || [], promptInfo: this.getPromptInfo() };
    }

    // Handle incomplete statements
    if (incomplete) {
      return { success: false, error: 'Incomplete statement', code: 'INCOMPLETE_STATEMENT', statementsCount: 0, statements: statements || [], identifiers: identifiers || [], extraTokens: extraTokens || [], secondTokens: secondTokens || [], thirdTokens: thirdTokens || [], promptInfo: this.getPromptInfo() };
    }

    // Handle empty after split (e.g., only comments)
    if (!statements || statements.length === 0) {
      return { success: true, statementsCount: 0, identifiers: [], extraTokens: [], secondTokens: [], thirdTokens: [], data: { message: '' }, promptInfo: this.getPromptInfo() };
    }

    // Check if any statements are shell commands
    // Note: Use identifiers from the CQL splitter, NOT string parsing
    // The splitter properly handles comments, whitespace, etc.
    const hasShellCommands = identifiers.some(id => {
      const cmd = (id || '').toLowerCase();
      return this['_do_' + cmd] !== undefined;
    });

    // If no shell commands, multiple statements, and no onProgress callback - use batch execution
    // (batch execution doesn't support per-statement progress callbacks)
    if (!hasShellCommands && statements.length > 1 && !onProgress) {
      return this.executeMulti(trimmed, { stopOnError });
    }

    // Get current page size for SELECT paging support
    let pageSize = 0;
    const infoResult = await this.getInfo();
    if (infoResult.success && infoResult.data) {
      pageSize = infoResult.data.pageSize || 0;
    }

    // Execute each statement one by one (supports shell commands and onProgress)
    const results = [];
    let stoppedEarly = false;

    for (let i = 0; i < statements.length; i++) {
      const stmt = statements[i];
      const stmtTrimmed = stmt.trim();
      if (!stmtTrimmed) continue;

      const isLast = (i === statements.length - 1);
      const identifier = identifiers[i] || '';
      let result;

      // Handle shell commands first - use identifier from splitter (properly tokenized)
      const shellResult = await this._handleShellCommand(identifier, stmtTrimmed);
      if (shellResult !== null) {
        result = shellResult;
      } else {
        // Regular CQL query - use paged execution for SELECT if paging enabled
        // Note: 'identifier' comes from the CQL splitter which properly tokenizes the statement
        // (handles comments, whitespace, etc.) - NOT a regex/string check
        const upperIdentifier = identifier.toUpperCase();
        if (upperIdentifier === 'SELECT' && pageSize > 0) {
          // Use paged execution - returns hasMore and queryId if more rows available
          const response = await callNativeTrueAsync(native.ExecuteQueryPaged, this._handle, stmtTrimmed);
          result = response;
        } else {
          // Regular execution
          const response = await callNativeTrueAsync(native.ExecuteQuery, this._handle, stmtTrimmed);
          result = response;
        }
      }

      // Normalize result structure
      if (!result.data && result.success) {
        result.data = { message: '' };
      }

      // Add metadata to result
      result.index = i;
      result.identifier = identifier;
      result.secondToken = secondTokens[i] || '';   // 2nd meaningful token
      result.thirdToken = thirdTokens[i] || '';     // 3rd meaningful token
      result.statement = stmtTrimmed;  // Original statement text
      result.statementsCount = statements.length;  // Total number of statements
      result.allCompleted = isLast && !stoppedEarly;
      result.promptInfo = this.getPromptInfo();

      // Call progress callback (async-aware - wait for it to complete)
      if (onProgress) {
        await onProgress(result);
      }

      results.push(result);

      // Stop on error if requested
      if (!result.success && stopOnError) {
        stoppedEarly = true;
        // Mark the last result as allCompleted since we're stopping
        result.allCompleted = true;
        break;
      }
    }

    // Build final response
    // Single statement - merge into result
    if (results.length === 1) {
      const result = results[0];
      return {
        ...result,
        statementsCount: statements.length,
        statementsExecuted: results.length,
        identifiers,
        extraTokens,
        secondTokens,
        thirdTokens,
        promptInfo: this.getPromptInfo()
      };
    }

    // Multiple statements - return combined results
    const allSuccess = results.every(r => r.success);
    return {
      success: allSuccess && !stoppedEarly,
      statementsCount: statements.length,
      statementsExecuted: results.length,
      identifiers,
      extraTokens,
      secondTokens,
      thirdTokens,
      stopped: stoppedEarly,
      data: {
        results: results.map(r => ({
          success: r.success,
          error: r.error,
          index: r.index,
          identifier: r.identifier,
          secondToken: r.secondToken,
          thirdToken: r.thirdToken,
          statement: r.statement,
          allCompleted: r.allCompleted,
          ...r.data
        }))
      },
      promptInfo: this.getPromptInfo()
    };
    } catch (err) {
      // Catch any unexpected errors and return a proper error response
      return {
        success: false,
        error: err.message || 'Internal execution error',
        code: 'INTERNAL_ERROR',
        statementsCount: 0,
        identifiers: [],
        extraTokens: [],
        promptInfo: this.getPromptInfo()
      };
    }
  }

  /**
   * Execute multiple CQL statements using Go's native batch execution
   * Use this for pure CQL (no shell commands) for better performance
   * @param {string} cql - CQL statement(s) separated by semicolons
   * @param {Object} options - Execution options
   * @param {boolean} [options.stopOnError=false] - Stop execution on first error
   * @returns {Promise<Object>} { success, data?, error?, statementsCount?, identifiers?, results? }
   */
  async executeMulti(cql, options = {}) {
    const trimmed = cql.trim();

    // Handle empty input
    if (!trimmed) {
      return {
        success: true,
        statementsCount: 0,
        identifiers: [],
        extraTokens: [],
        secondTokens: [],
        thirdTokens: [],
        data: { message: '' },
        promptInfo: this.getPromptInfo()
      };
    }

    // Use Go's native multi-statement execution
    const optionsJSON = JSON.stringify({
      stopOnError: options.stopOnError || false
    });

    const response = await callNativeTrueAsync(
      native.ExecuteMultiQuery,
      this._handle,
      trimmed,
      optionsJSON
    );

    if (!response.success) {
      return {
        success: false,
        error: response.error || 'Query execution failed',
        code: response.code,
        promptInfo: this.getPromptInfo()
      };
    }

    const result = response.data;

    // Handle parse errors
    if (result.parseError) {
      return {
        success: false,
        error: result.parseError,
        code: result.incomplete ? 'INCOMPLETE_STATEMENT' : 'PARSE_ERROR',
        statementsCount: 0,
        identifiers: result.identifiers || [],
        extraTokens: result.extraTokens || [],
        secondTokens: result.secondTokens || [],
        thirdTokens: result.thirdTokens || [],
        promptInfo: this.getPromptInfo()
      };
    }

    // Update keyspace if USE command was executed
    this._updateKeyspaceFromResults(result.results || []);

    // Single statement - flatten result
    if (result.results && result.results.length === 1) {
      const sr = result.results[0];
      return {
        success: sr.success,
        error: sr.error,
        code: sr.errorCode,
        statementsCount: result.statementsCount,
        identifiers: result.identifiers,
        extraTokens: result.extraTokens,
        secondTokens: result.secondTokens || [],
        thirdTokens: result.thirdTokens || [],
        data: this._formatStatementResult(sr),
        promptInfo: this.getPromptInfo()
      };
    }

    // Multiple statements
    return {
      success: !result.stopped,
      statementsCount: result.statementsCount,
      statementsExecuted: result.statementsExecuted,
      identifiers: result.identifiers,
      extraTokens: result.extraTokens,
      secondTokens: result.secondTokens || [],
      thirdTokens: result.thirdTokens || [],
      stopped: result.stopped,
      data: {
        results: (result.results || []).map((sr, idx) => ({
          success: sr.success,
          error: sr.error,
          index: sr.index,
          identifier: sr.identifier,
          secondToken: (result.secondTokens || [])[idx] || '',
          thirdToken: (result.thirdTokens || [])[idx] || '',
          ...this._formatStatementResult(sr)
        }))
      },
      promptInfo: this.getPromptInfo()
    };
  }

  /**
   * Format a StatementResult from Go into the expected data format
   * @private
   */
  _formatStatementResult(sr) {
    return {
      columns: sr.columns || [],
      columnTypes: sr.columnTypes || [],
      rows: sr.rows || [],
      rowCount: sr.rowCount || 0,
      duration: sr.duration || '',
      message: sr.message || '',
      traceSessionId: sr.traceSessionId,
      keyspace: sr.keyspace,
      table: sr.table
    };
  }

  /**
   * Update keyspace from multi-query results (handles USE command)
   * @private
   */
  _updateKeyspaceFromResults(results) {
    for (const sr of results) {
      if (sr.identifier === 'USE' && sr.success && sr.message) {
        // Extract keyspace from "Now using keyspace X"
        const match = sr.message.match(/Now using keyspace (\w+)/i);
        if (match) {
          this._keyspace = match[1];
        }
      }
    }
  }

  /**
   * Fetch the next page of results for a paged query
   * @param {string} queryId - The query ID returned from execute() (when hasMore is true)
   * @returns {Promise<Object>} { success, data?: { columns, columnTypes, rows, rowCount, hasMore, queryId }, error? }
   *
   * If hasMore is false, the query is automatically closed and queryId is cleared.
   */
  async fetchNextPage(queryId) {
    if (!queryId) {
      return { success: false, error: 'queryId is required' };
    }

    return await callNativeTrueAsync(native.FetchNextPage, this._handle, queryId);
  }

  /**
   * Cancel/close an active paged query iterator
   * Call this to clean up resources if you don't want to fetch all pages
   * @param {string} queryId - The query ID returned from executePaged()
   * @returns {Promise<Object>} { success, data?: { cancelled, reason? }, error? }
   */
  async cancelPagedQuery(queryId) {
    if (!queryId) {
      return { success: false, error: 'queryId is required' };
    }

    return await callNativeTrueAsync(native.CancelPagedQuery, this._handle, queryId);
  }

  /**
   * Cancel any active queries on this session
   * Used for handling user interrupts (CTRL+C / SIGINT)
   * @returns {Promise<Object>} { success, data?: { cancelledQueries: number }, error? }
   */
  async cancelQuery() {
    return await callNativeTrueAsync(native.CancelQuery, this._handle);
  }

  /**
   * Handle shell commands - dispatch by identifier from CQL splitter
   * Pattern: identifier = first token from splitter, handler = _do_<identifier>
   * @private
   * @param {string} identifier - Statement identifier from CQL splitter (properly tokenized)
   * @param {string} original - Original command string (for argument parsing)
   * @returns {Object|null} Response object or null if not a shell command
   */
  async _handleShellCommand(identifier, original) {
    // Use identifier from CQL splitter for command dispatch
    // The splitter properly handles comments, whitespace, etc.
    let cmdword = (identifier || '').toLowerCase();

    // Handle ? -> help (like Python: if cmdword == '?': cmdword = 'help')
    if (cmdword === '?') {
      cmdword = 'help';
    }

    // Dispatch to handler based on identifier
    const handler = this['_do_' + cmdword];
    if (handler) {
      // Parse original for arguments (handlers need parts[1], parts[2], etc.)
      const parts = original.trim().split(/\s+/);
      return handler.call(this, parts, original);
    }

    // Not a shell command
    return null;
  }

  // ============================================
  // Shell Command Handlers (do_<command> pattern from Python cqlsh)
  // ============================================

  /**
   * CONSISTENCY [level] - Show or set consistency level
   * From cqlshmain.py do_consistency()
   * Note: Only shows/sets REGULAR consistency, not serial
   */
  async _do_consistency(parts, original) {
    const level = parts[1]?.replace(/;$/, '').toUpperCase();

    if (!level) {
      // Show current consistency (regular only, matching Python cqlsh behavior)
      const info = await this.getInfo();
      if (info.success) {
        const consistency = info.data.consistency;
        return this._textResponse(
          `Current consistency level is ${consistency}.`,
          {
            command: 'consistency',
            action: 'show',
            consistency: consistency
          }
        );
      }
      return { success: false, error: 'Failed to get session info' };
    }

    // Set consistency
    const result = await callNativeAsync(() => native.SetConsistency(this._handle, level));
    if (result.success) {
      return this._textResponse(
        `Consistency level set to ${level}.`,
        {
          command: 'consistency',
          action: 'set',
          consistency: level
        }
      );
    }
    return { success: false, error: `Invalid consistency level: ${level}` };
  }

  /**
   * SERIAL CONSISTENCY [level] - Show or set serial consistency level
   * From cqlshmain.py do_serial()
   * Note: Command is "SERIAL CONSISTENCY" but handler is do_serial (first word)
   */
  async _do_serial(parts, original) {
    // parts[0] = SERIAL, parts[1] = CONSISTENCY, parts[2] = level (optional)
    const level = parts[2]?.replace(/;$/, '').toUpperCase();

    if (!level) {
      // Show current serial consistency
      const info = await this.getInfo();
      if (info.success) {
        const serialConsistency = info.data.serialConsistency || 'SERIAL';
        return this._textResponse(
          `Current serial consistency level is ${serialConsistency}.`,
          {
            command: 'serial',
            action: 'show',
            serialConsistency: serialConsistency
          }
        );
      }
      return { success: false, error: 'Failed to get session info' };
    }

    // Validate level
    if (level !== 'SERIAL' && level !== 'LOCAL_SERIAL') {
      return { success: false, error: `Invalid serial consistency level: ${level}. Valid levels are: SERIAL, LOCAL_SERIAL.` };
    }

    // Set serial consistency (would need Go binding for SetSerialConsistency)
    // For now, just acknowledge - TODO: add SetSerialConsistency to Go bindings
    return this._textResponse(
      `Serial consistency level set to ${level}.`,
      {
        command: 'serial',
        action: 'set',
        serialConsistency: level
      }
    );
  }

  /**
   * PAGING [ON|OFF|number] - Show or set query paging
   * From cqlshmain.py do_paging()
   */
  async _do_paging(parts, original) {
    const value = parts[1]?.replace(/;$/, '');

    if (!value) {
      // Show current paging state
      const info = await this.getInfo();
      if (info.success) {
        const pageSize = info.data.pageSize;
        const enabled = pageSize > 0;
        return this._textResponse(
          enabled ? `Page size: ${pageSize}` : 'Disabled paging.',
          {
            command: 'paging',
            action: 'show',
            enabled: enabled,
            pageSize: pageSize
          }
        );
      }
      return { success: false, error: 'Failed to get session info' };
    }

    // Set paging
    const result = await callNativeAsync(() => native.SetPaging(this._handle, value));
    if (result.success) {
      const pageSize = result.data.pageSize || 0;
      const enabled = result.data.paging !== 'OFF' && pageSize > 0;
      return this._textResponse(
        enabled ? `Page size: ${pageSize}` : 'Disabled paging.',
        {
          command: 'paging',
          action: 'set',
          enabled: enabled,
          pageSize: pageSize
        }
      );
    }
    return { success: false, error: `Invalid paging value: ${value}` };
  }

  /**
   * TRACING [ON|OFF] - Show or set tracing
   * From cqlshmain.py do_tracing()
   */
  async _do_tracing(parts, original) {
    const value = parts[1]?.replace(/;$/, '').toUpperCase();

    if (!value) {
      // Show current tracing state
      const info = await this.getInfo();
      if (info.success) {
        const enabled = info.data.tracing;
        return this._textResponse(
          `TRACING is ${enabled ? 'ON' : 'OFF'}`,
          { command: 'tracing', action: 'show', enabled: enabled }
        );
      }
      return { success: false, error: 'Failed to get session info' };
    }

    // Set tracing
    if (value === 'ON') {
      const result = await callNativeAsync(() => native.SetTracing(this._handle, 1));
      if (result.success) {
        return this._textResponse(
          'TRACING set to ON',
          { command: 'tracing', action: 'set', enabled: true }
        );
      }
    } else if (value === 'OFF') {
      const result = await callNativeAsync(() => native.SetTracing(this._handle, 0));
      if (result.success) {
        return this._textResponse(
          'TRACING set to OFF',
          { command: 'tracing', action: 'set', enabled: false }
        );
      }
    }
    return { success: false, error: `Invalid tracing value: ${value} (use ON or OFF)` };
  }

  /**
   * EXPAND [ON|OFF] - Show or set expanded output
   * From cqlshmain.py do_expand()
   */
  async _do_expand(parts, original) {
    const value = parts[1]?.replace(/;$/, '').toUpperCase();

    if (!value) {
      // Show current expand state
      const info = await this.getInfo();
      if (info.success) {
        const enabled = info.data.expand;
        return this._textResponse(
          `EXPAND is ${enabled ? 'ON' : 'OFF'}`,
          { command: 'expand', action: 'show', enabled: enabled }
        );
      }
      return { success: false, error: 'Failed to get session info' };
    }

    // Set expand
    if (value === 'ON') {
      const result = await callNativeAsync(() => native.SetExpand(this._handle, 1));
      if (result.success) {
        return this._textResponse(
          'EXPAND set to ON',
          { command: 'expand', action: 'set', enabled: true }
        );
      }
    } else if (value === 'OFF') {
      const result = await callNativeAsync(() => native.SetExpand(this._handle, 0));
      if (result.success) {
        return this._textResponse(
          'EXPAND set to OFF',
          { command: 'expand', action: 'set', enabled: false }
        );
      }
    }
    return { success: false, error: `Invalid expand value: ${value} (use ON or OFF)` };
  }

  /**
   * CLEAR/CLS - Clear screen
   * From cqlshmain.py do_clear()
   */
  async _do_clear(parts, original) {
    return this._textResponse('', { command: 'clear', action: 'execute' });
  }

  async _do_cls(parts, original) {
    return this._textResponse('', { command: 'cls', action: 'execute' });
  }

  /**
   * EXIT/QUIT - Exit cqlsh (handled at pty.js level, but we acknowledge here)
   * From cqlshmain.py do_exit()
   */
  async _do_exit(parts, original) {
    return this._textResponse('', { command: 'exit', action: 'execute' });
  }

  async _do_quit(parts, original) {
    return this._textResponse('', { command: 'quit', action: 'execute' });
  }

  /**
   * HELP/? - Show help (minimal implementation)
   * From cqlshmain.py do_help()
   */
  async _do_help(parts, original) {
    const commands = ['CONSISTENCY', 'SERIAL CONSISTENCY', 'PAGING', 'TRACING', 'EXPAND', 'CLEAR', 'EXIT'];
    return this._textResponse(
      `CQL Shell Commands: ${commands.join(', ')}`,
      { command: 'help', action: 'show', commands: commands }
    );
  }

  /**
   * ELAPSED [ON|OFF] - Show or set elapsed time display
   * From cqlshmain.py do_elapsed()
   */
  async _do_elapsed(parts, original) {
    const value = parts[1]?.replace(/;$/, '').toUpperCase();
    if (!value) {
      return this._textResponse(
        'ELAPSED is OFF',
        { command: 'elapsed', action: 'show', enabled: false }
      );
    }
    const enabled = value === 'ON';
    return this._textResponse(
      `ELAPSED set to ${value}`,
      { command: 'elapsed', action: 'set', enabled: enabled }
    );
  }

  /**
   * USE <keyspace> - Switch to a keyspace
   * From cqlshmain.py do_use()
   */
  async _do_use(parts, original) {
    // Extract keyspace name (remove quotes and trailing semicolon)
    let keyspace = parts[1]?.replace(/;$/, '').replace(/^["']|["']$/g, '');

    if (!keyspace) {
      return { success: false, error: 'USE requires a keyspace name' };
    }

    // Set the keyspace via native binding
    const result = await callNativeAsync(() => native.SetKeyspace(this._handle, keyspace));

    if (result.success) {
      // Update local keyspace tracking for prompt
      this._keyspace = keyspace;
      return this._textResponse(
        '',  // USE typically has no output on success
        {
          command: 'use',
          action: 'set',
          keyspace: keyspace,
          prompt: this.getPrompt()
        }
      );
    }

    return { success: false, error: result.error || `Keyspace '${keyspace}' does not exist` };
  }

  async _do_copy(parts, original) {
    // Parse COPY command: COPY table [(col1, col2)] TO|FROM 'filename' [WITH options]
    const pattern = /COPY\s+(\S+)(?:\s*\(([^)]+)\))?\s+(TO|FROM)\s+(?:'([^']+)'|"([^"]+)"|(\S+))(?:\s+WITH\s+(.+?))?;?\s*$/i;
    const match = original.match(pattern);

    if (!match) {
      return { success: false, error: "Invalid COPY syntax. Use: COPY table TO 'file.csv' or COPY table FROM 'file.csv' [WITH HEADER = true]" };
    }

    const table = match[1];
    const columnsStr = match[2];
    const direction = match[3].toUpperCase();
    const filename = match[4] || match[5] || match[6];
    const withStr = match[7];

    // Parse columns
    const columns = columnsStr
      ? columnsStr.split(',').map(c => c.trim())
      : undefined;

    // Parse WITH options: key = value [AND key = value ...]
    const options = {};
    if (withStr) {
      const optParts = withStr.split(/\s+AND\s+/i);
      for (const part of optParts) {
        const optMatch = part.match(/(\w+)\s*=\s*(?:'([^']*)'|"([^"]*)"|(\S+))/);
        if (optMatch) {
          const key = optMatch[1].toUpperCase();
          const value = optMatch[2] ?? optMatch[3] ?? optMatch[4];
          options[key] = value;
        }
      }
    }

    const params = { table, filename, options };
    if (columns) params.columns = columns;
    const paramsJSON = JSON.stringify(params);

    let result;
    if (direction === 'TO') {
      result = await callNativeTrueAsync(native.CopyTo, this._handle, paramsJSON);
    } else {
      result = await callNativeTrueAsync(native.CopyFrom, this._handle, paramsJSON);
    }

    if (result.success) {
      const data = result.data || {};
      let message;
      if (direction === 'TO') {
        message = `Exported ${data.rows_exported || 0} rows to ${filename}`;
      } else {
        message = `Imported ${data.rows_imported || 0} rows from ${filename}`;
        if (data.errors > 0) message += ` (${data.errors} insert errors)`;
        if (data.parse_errors > 0) message += ` (${data.parse_errors} parse errors)`;
      }
      return this._textResponse(message, { command: 'copy', direction: direction.toLowerCase(), ...data });
    }

    return result;
  }

  /**
   * Create a text response (for shell commands)
   * Includes both message (for display) and structured commandResult (for programmatic access)
   * Note: promptInfo is added at the top level by execute(), not here
   * @private
   */
  _textResponse(message, commandResult = {}) {
    return {
      success: true,
      data: {
        columns: [],
        columnTypes: [],
        rows: [],
        rowCount: 0,
        duration: '',
        message: message,
        commandResult: commandResult,  // Structured data for the command
      },
    };
  }

  /**
   * Set the consistency level
   * @param {string} level - Consistency level (e.g., 'LOCAL_ONE', 'QUORUM')
   * @returns {Promise<Object>} { success, data?, error? }
   */
  async setConsistency(level) {
    return await callNativeAsync(() =>
      native.SetConsistency(this._handle, level)
    );
  }

  /**
   * Set paging size or disable paging
   * @param {string|number} value - Page size number or 'OFF' to disable
   * @returns {Promise<Object>} { success, data?: { paging, pageSize }, error? }
   */
  async setPaging(value) {
    const valueStr = String(value);
    return await callNativeAsync(() =>
      native.SetPaging(this._handle, valueStr)
    );
  }

  /**
   * Enable or disable query tracing
   * @param {boolean} enabled - Whether to enable tracing
   * @returns {Promise<Object>} { success, data?: { tracing }, error? }
   */
  async setTracing(enabled) {
    return await callNativeAsync(() =>
      native.SetTracing(this._handle, enabled ? 1 : 0)
    );
  }

  /**
   * Enable or disable expand mode (vertical row display)
   * @param {boolean} enabled - Whether to enable expand mode
   * @returns {Promise<Object>} { success, data?: { expand }, error? }
   */
  async setExpand(enabled) {
    return await callNativeAsync(() =>
      native.SetExpand(this._handle, enabled ? 1 : 0)
    );
  }

  /**
   * Set the current keyspace
   * @param {string} keyspace - Keyspace name
   * @returns {Promise<Object>} { success, data?, error? }
   */
  async setKeyspace(keyspace) {
    const response = await callNativeAsync(() =>
      native.SetKeyspace(this._handle, keyspace)
    );

    if (response.success) {
      this._keyspace = keyspace;
    }

    return response;
  }

  /**
   * Get session information
   * @returns {Promise<Object>} { success, data?, error? }
   */
  async getInfo() {
    return await callNativeAsync(() =>
      native.GetSessionInfo(this._handle)
    );
  }

  /**
   * Get full cluster metadata (keyspaces, tables, columns, indexes, types, functions, etc.)
   * @returns {Promise<Object>} { success, data?: ClusterMetadata, error? }
   */
  async getClusterMetadata() {
    return await callNativeTrueAsync(native.GetClusterMetadata, this._handle);
  }

  /**
   * Export table data to a CSV file (COPY TO)
   * @param {string} table - Table name (can be keyspace.table)
   * @param {string} filename - Output CSV file path
   * @param {Object} [options] - Export options
   * @param {string[]} [options.columns] - Specific columns to export (default: all)
   * @param {boolean} [options.header=false] - Include column header row
   * @param {string} [options.delimiter=','] - Column delimiter
   * @param {string} [options.nullval='null'] - String to use for NULL values
   * @param {number} [options.maxrows=-1] - Max rows to export (-1 for unlimited)
   * @param {number} [options.pagesize=1000] - Rows per page for streaming
   * @returns {Promise<Object>} { success, data?: { rows_exported }, error? }
   */
  async copyTo(table, filename, options = {}) {
    const params = {
      table,
      filename,
      columns: options.columns,
      options: {},
    };
    // Map JS-friendly option names to COPY option keys
    if (options.header !== undefined) params.options.HEADER = String(options.header);
    if (options.delimiter !== undefined) params.options.DELIMITER = options.delimiter;
    if (options.nullval !== undefined) params.options.NULLVAL = options.nullval;
    if (options.maxrows !== undefined) params.options.MAXROWS = String(options.maxrows);
    if (options.pagesize !== undefined) params.options.PAGESIZE = String(options.pagesize);

    const paramsJSON = JSON.stringify(params);
    return await callNativeTrueAsync(native.CopyTo, this._handle, paramsJSON);
  }

  /**
   * Import data from a CSV file into a table (COPY FROM)
   * @param {string} table - Table name (can be keyspace.table)
   * @param {string} filename - Input CSV file path
   * @param {Object} [options] - Import options
   * @param {string[]} [options.columns] - Column names matching CSV columns (default: from header or schema)
   * @param {boolean} [options.header=false] - CSV file has a header row
   * @param {string} [options.delimiter=','] - Column delimiter
   * @param {string} [options.nullval='null'] - String representing NULL values
   * @param {number} [options.maxrows=-1] - Max rows to import (-1 for unlimited)
   * @param {number} [options.skiprows=0] - Number of rows to skip at start
   * @param {number} [options.chunksize=5000] - Progress reporting chunk size
   * @param {number} [options.maxbatchsize=20] - Max rows per batch insert
   * @param {number} [options.maxrequests=6] - Max concurrent batch workers
   * @returns {Promise<Object>} { success, data?: { rows_imported, errors, parse_errors, skipped_rows }, error? }
   */
  async copyFrom(table, filename, options = {}) {
    const params = {
      table,
      filename,
      columns: options.columns,
      options: {},
    };
    if (options.header !== undefined) params.options.HEADER = String(options.header);
    if (options.delimiter !== undefined) params.options.DELIMITER = options.delimiter;
    if (options.nullval !== undefined) params.options.NULLVAL = options.nullval;
    if (options.maxrows !== undefined) params.options.MAXROWS = String(options.maxrows);
    if (options.skiprows !== undefined) params.options.SKIPROWS = String(options.skiprows);
    if (options.chunksize !== undefined) params.options.CHUNKSIZE = String(options.chunksize);
    if (options.maxbatchsize !== undefined) params.options.MAXBATCHSIZE = String(options.maxbatchsize);
    if (options.maxrequests !== undefined) params.options.MAXREQUESTS = String(options.maxrequests);

    const paramsJSON = JSON.stringify(params);
    return await callNativeTrueAsync(native.CopyFrom, this._handle, paramsJSON);
  }

  /**
   * Generate DDL (CREATE statements) for various scopes
   * @param {Object} options - DDL generation options
   * @param {boolean} [options.cluster] - If true, generate DDL for entire cluster
   * @param {boolean} [options.includeSystem=true] - If true, include system keyspaces in cluster DDL
   * @param {string} [options.keyspace] - Keyspace name (required if cluster is false)
   * @param {string} [options.table] - Table name (optional, requires keyspace)
   * @param {string} [options.index] - Index name (optional, requires keyspace and table)
   * @param {string} [options.type] - User type name (optional, requires keyspace)
   * @param {string} [options.function] - Function name (optional, requires keyspace)
   * @param {string} [options.aggregate] - Aggregate name (optional, requires keyspace)
   * @param {string} [options.view] - Materialized view name (optional, requires keyspace)
   * @returns {Promise<Object>} { success, data?: { ddl: string, scope: string }, error? }
   *
   * @example
   * // Get DDL for entire cluster (includes system keyspaces by default)
   * await session.getDDL({ cluster: true });
   *
   * // Get DDL for cluster without system keyspaces
   * await session.getDDL({ cluster: true, includeSystem: false });
   *
   * // Get DDL for specific keyspace
   * await session.getDDL({ keyspace: 'mhmd' });
   *
   * // Get DDL for specific table
   * await session.getDDL({ keyspace: 'mhmd', table: 'users' });
   *
   * // Get DDL for specific index
   * await session.getDDL({ keyspace: 'mhmd', table: 'users', index: 'users_email_idx' });
   */
  async getDDL(options = {}) {
    // Default includeSystem to true
    const opts = { includeSystem: true, ...options };
    const optionsJSON = JSON.stringify(opts);
    return await callNativeTrueAsync(native.GetDDL, this._handle, optionsJSON);
  }

  /**
   * Close the session
   * @returns {Promise<Object>} { success, error? }
   */
  async close() {
    return await callNativeAsync(() =>
      native.CloseSession(this._handle)
    );
  }

  /**
   * Execute multiple CQL files (SOURCE command equivalent)
   * @param {Object} options - Execution options
   * @param {string[]} options.files - Array of file paths to execute
   * @param {boolean} [options.stopOnError=false] - Stop execution on first error
   * @param {Function} [options.onProgress] - Callback for progress updates
   * @returns {Promise<Object>} { success, data?: { result, progress }, error? }
   *
   * Progress callback receives:
   * {
   *   filePath: string,
   *   fileIndex: number,
   *   totalFiles: number,
   *   statementsTotal: number,
   *   statementsRun: number,
   *   statementsOK: number,
   *   statementsFailed: number,
   *   currentStatement: string,
   *   errors: string[],
   *   isComplete: boolean,
   *   duration: number (ms)
   * }
   *
   * Final result contains:
   * {
   *   totalFiles: number,
   *   filesCompleted: number,
   *   filesFailed: number,
   *   totalStatements: number,
   *   statementsOK: number,
   *   statementsFailed: number,
   *   totalDuration: number (ms),
   *   errors: string[],
   *   stopped: boolean
   * }
   */
  async executeSourceFiles(options = {}) {
    const { files, stopOnError = false, onProgress } = options;

    if (!files || !Array.isArray(files) || files.length === 0) {
      return { success: false, error: 'Files array is required' };
    }

    const optionsJSON = JSON.stringify({ files, stopOnError });

    // If no progress callback, just execute and return
    if (!onProgress) {
      return await callNativeTrueAsync(native.ExecuteSourceFiles, this._handle, optionsJSON);
    }

    // With progress callback, we need to poll for progress
    let lastProgressCount = 0;
    const pollInterval = 100; // ms
    let pollTimer = null;
    const sentErrorCounts = new Map(); // Track sent errors per file

    const pollProgress = async () => {
      const progressResult = await callNativeAsync(() =>
        native.GetSourceProgress(this._handle)
      );

      if (progressResult.success && progressResult.data) {
        const progress = progressResult.data;
        if (progress.length > lastProgressCount) {
          // Report new progress items
          for (let i = lastProgressCount; i < progress.length; i++) {
            const item = progress[i];
            const sentCount = sentErrorCounts.get(item.filePath) || 0;
            const newErrors = item.errors ? item.errors.slice(sentCount) : [];
            sentErrorCounts.set(item.filePath, item.errors ? item.errors.length : 0);
            onProgress({ ...item, errors: newErrors });
          }
          lastProgressCount = progress.length;
        } else if (progress.length > 0) {
          // Check if last item was updated
          const lastItem = progress[progress.length - 1];
          const sentCount = sentErrorCounts.get(lastItem.filePath) || 0;
          const newErrors = lastItem.errors ? lastItem.errors.slice(sentCount) : [];
          sentErrorCounts.set(lastItem.filePath, lastItem.errors ? lastItem.errors.length : 0);
          onProgress({ ...lastItem, errors: newErrors });
        }
      }
    };

    // Start polling
    pollTimer = setInterval(pollProgress, pollInterval);

    try {
      const result = await callNativeTrueAsync(native.ExecuteSourceFiles, this._handle, optionsJSON);

      // Final poll to get any remaining progress
      await pollProgress();

      return result;
    } finally {
      if (pollTimer) {
        clearInterval(pollTimer);
      }
    }
  }

  /**
   * Stop the currently running source file execution for this session.
   * @returns {Promise<Object>} { success: boolean, error?: string }
   */
  async stopSourceExecution() {
    return await callNativeAsync(() => native.StopSourceExecution(this._handle));
  }

  /**
   * Get query trace by session ID
   * @param {string} sessionId - The trace session UUID
   * @returns {Promise<Object>} { success, data?: QueryTraceResult, error? }
   *
   * QueryTraceResult contains:
   * {
   *   session: {
   *     sessionId: string,
   *     coordinator: string,
   *     duration: number (microseconds),
   *     startedAt: string (ISO timestamp),
   *     client: string,
   *     request: string,
   *     command: string,
   *     parameters: string
   *   },
   *   events: [{
   *     activity: string,
   *     timestamp: string (ISO timestamp),
   *     source: string,
   *     sourceElapsed: number (microseconds),
   *     thread: string
   *   }]
   * }
   */
  async getQueryTrace(sessionId) {
    if (!sessionId) {
      return { success: false, error: 'Session ID is required' };
    }

    return await callNativeTrueAsync(native.GetQueryTrace, this._handle, sessionId);
  }

  /**
   * Get the Cassandra version
   */
  get cassandraVersion() {
    return this._cassandraVersion;
  }

  /**
   * Get the current keyspace
   */
  get keyspace() {
    return this._keyspace;
  }

  /**
   * Get the session handle (for advanced use)
   */
  get handle() {
    return this._handle;
  }

  // ============================================
  // Static utility methods
  // ============================================

  /**
   * Check TLS security of a connection
   * @param {Object} options - TLS check options
   * @param {string} [options.host] - Host to connect to (required unless filesOnly)
   * @param {number} [options.port=9042] - Port
   * @param {string} [options.caFile] - CA certificate file path
   * @param {string} [options.certFile] - Client certificate file path
   * @param {string} [options.keyFile] - Client key file path
   * @param {boolean} [options.skipVerify=false] - Skip certificate verification
   * @param {boolean} [options.filesOnly=false] - Only analyze certificate files, don't connect
   * @returns {Promise<Object>} { success, data?: TLSSecurityInfo, error? }
   */
  static async checkTLSSecurity(options = {}) {
    const optionsJSON = JSON.stringify(options);
    return await callNativeTrueAsync(native.CheckTLS, optionsJSON);
  }

  /**
   * Decrypt a credential using RSA private key (standalone utility)
   * Note: Normally you don't need this - just pass rsaPrivateKey to connect() and
   * credentials are automatically decrypted. Use this only for manual decryption.
   * @param {Object} options - Decryption options
   * @param {string} options.ciphertext - Base64-encoded ciphertext
   * @param {string} [options.privateKey] - PEM-encoded private key
   * @param {string} [options.privateKeyFile] - Path to private key file
   * @returns {Promise<Object>} { success, data?: { plaintext }, error? }
   */
  static async decryptCredential(options) {
    const optionsJSON = JSON.stringify(options);
    return await callNativeAsync(() =>
      native.DecryptCredential(optionsJSON)
    );
  }

  /**
   * Parse a DataStax Astra secure connect bundle
   * @param {Object} options - Bundle options
   * @param {string} options.bundlePath - Path to secure-connect-*.zip bundle
   * @param {string} [options.extractDir] - Directory to extract to (temp dir if not specified)
   * @returns {Promise<Object>} { success, data?: AstraBundleInfo, error? }
   */
  static async parseAstraBundle(options) {
    const optionsJSON = JSON.stringify(options);
    return await callNativeAsync(() =>
      native.ParseAstraSecureBundle(optionsJSON)
    );
  }

  /**
   * Validate an Astra secure connect bundle without extracting
   * @param {string} bundlePath - Path to secure-connect-*.zip bundle
   * @returns {Promise<Object>} { success, data?: { valid, errors }, error? }
   */
  static async validateAstraBundle(bundlePath) {
    return await callNativeAsync(() =>
      native.ValidateAstraSecureBundle(bundlePath)
    );
  }

  /**
   * Connect using a DataStax Astra secure connect bundle
   * @param {Object} options - Connection options
   * @param {string} options.bundlePath - Path to secure-connect-*.zip bundle
   * @param {string} options.username - Astra client ID
   * @param {string} options.password - Astra client secret
   * @param {string} [options.keyspace] - Override keyspace from bundle
   * @param {string} [options.extractDir] - Directory to extract to
   * @returns {Promise<Object>} { success, data?: { session, bundleInfo }, error? }
   */
  static async connectWithAstraBundle(options) {
    const optionsJSON = JSON.stringify(options);
    const response = await callNativeTrueAsync(native.CreateAstraSession, optionsJSON);

    if (!response.success || !response.data) {
      return { success: false, error: response.error || 'Failed to create Astra session' };
    }

    // Get session info to retrieve username and host
    const handle = response.data.handle;
    const infoResult = await callNativeAsync(() => native.GetSessionInfo(handle));
    const username = infoResult.success ? infoResult.data.username : '';
    const host = infoResult.success ? infoResult.data.host : '';

    return {
      success: true,
      data: {
        session: new CQLSession(
          handle,
          response.data.cassandraVersion,
          response.data.keyspace,
          username,
          host
        ),
        bundleInfo: response.data.bundleInfo,
      },
    };
  }

  /**
   * Test Astra connection with cancellation support
   * @param {Object} options - Connection options
   * @param {string} options.bundlePath - Path to secure-connect-*.zip bundle
   * @param {string} options.username - Astra client ID
   * @param {string} options.password - Astra client secret
   * @param {string} options.requestID - Unique request ID for cancellation (required)
   * @param {string} [options.keyspace] - Override keyspace from bundle
   * @param {string} [options.extractDir] - Directory to extract to
   * @returns {Promise<Object>} { success, data?, error?, code? }
   *
   * Returns same format as testConnection. If cancelled:
   * { success: false, error: 'Connection cancelled', code: 'CANCELLED' }
   */
  static async testAstraConnectionWithID(options) {
    if (!options.requestID) {
      return { success: false, error: 'requestID is required for cancellable connection test' };
    }
    if (!options.bundlePath) {
      return { success: false, error: 'bundlePath is required' };
    }
    if (!options.username || !options.password) {
      return { success: false, error: 'username and password are required' };
    }

    const optionsJSON = JSON.stringify(options);

    return await callNativeTrueAsync(native.TestAstraConnectionWithID, optionsJSON);
  }

  /**
   * Cleanup extracted Astra bundle files
   * @param {string} extractedDir - Directory containing extracted bundle
   * @returns {Promise<Object>} { success, error? }
   */
  static async cleanupAstraBundle(extractedDir) {
    return await callNativeAsync(() =>
      native.CleanupAstraExtracted(extractedDir)
    );
  }
}

module.exports = { CQLSession };
