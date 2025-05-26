/**
 * @fileoverview SQLite storage provider implementation
 * 
 * This provider implements persistent storage using SQLite via better-sqlite3.
 * It provides a unified backend for both session state and message history,
 * enabling a fully persistent, self-contained local AgentDock environment.
 */

import Database from 'better-sqlite3';
import { existsSync, mkdirSync } from 'fs';
import { dirname, resolve } from 'path';
import { logger, LogCategory } from '../../logging';
import { 
  StorageProvider, 
  StorageOptions, 
  ListOptions 
} from '../types';

/**
 * Configuration for SQLite storage provider
 */
export interface SQLiteStorageProviderConfig {
  /**
   * Path to the SQLite database file
   * Defaults to './agentdock_storage.sqlite'
   */
  dbPath?: string;
  
  /**
   * Namespace for this provider instance
   * Used to prefix keys in the database
   */
  namespace?: string;
  
  /**
   * Whether to enable WAL mode for better concurrent access
   * Defaults to true
   */
  enableWAL?: boolean;
  
  /**
   * Cleanup interval in milliseconds for expired items
   * Set to 0 to disable automatic cleanup
   * Defaults to 5 minutes (300000ms)
   */
  cleanupIntervalMs?: number;
}

/**
 * SQLite storage provider for persistent local storage
 * 
 * Uses a simple key-value table with TTL support and JSON serialization.
 * Implements the full StorageProvider interface including list operations.
 */
export class SQLiteStorageProvider implements StorageProvider {
  private db: Database.Database;
  private namespace: string;
  private cleanupInterval?: NodeJS.Timeout;
  private readonly providerType = 'sqlite';

  constructor(config: SQLiteStorageProviderConfig = {}) {
    const {
      dbPath = process.env.SQLITE_PATH || './agentdock_storage.sqlite',
      namespace = 'agentdock',
      enableWAL = true,
      cleanupIntervalMs = 5 * 60 * 1000 // 5 minutes
    } = config;

    this.namespace = namespace;

    // Ensure the directory exists
    const resolvedPath = resolve(dbPath);
    const dbDir = dirname(resolvedPath);
    if (!existsSync(dbDir)) {
      mkdirSync(dbDir, { recursive: true });
    }

    // Initialize database
    this.db = new Database(resolvedPath);
    
    // Enable WAL mode for better concurrent access
    if (enableWAL) {
      this.db.pragma('journal_mode = WAL');
    }

    // Create the key-value table
    this.initializeSchema();

    // Set up automatic cleanup if enabled
    if (cleanupIntervalMs > 0) {
      this.startCleanup(cleanupIntervalMs);
    }

    logger.info(LogCategory.STORAGE, this.providerType, 'Initialized SQLite storage provider', {
      dbPath: resolvedPath,
      namespace: this.namespace,
      enableWAL,
      cleanupIntervalMs
    });
  }

  /**
   * Initialize the database schema
   */
  private initializeSchema(): void {
    // Create the main key-value store table
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS key_value_store (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        expires_at INTEGER NULL,
        created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000),
        updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000),
        metadata TEXT NULL
      )
    `);

    // Create index on expires_at for efficient cleanup
    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_key_value_store_expires_at 
      ON key_value_store(expires_at) 
      WHERE expires_at IS NOT NULL
    `);

    // Create trigger to update updated_at timestamp
    this.db.exec(`
      CREATE TRIGGER IF NOT EXISTS update_key_value_store_updated_at
      AFTER UPDATE ON key_value_store
      BEGIN
        UPDATE key_value_store SET updated_at = strftime('%s', 'now') * 1000 WHERE key = NEW.key;
      END
    `);

    logger.debug(LogCategory.STORAGE, this.providerType, 'Database schema initialized');
  }

  /**
   * Generate a namespaced key
   */
  private getNamespacedKey(key: string, namespace?: string): string {
    const ns = namespace || this.namespace;
    return `${ns}:${key}`;
  }

  /**
   * Start automatic cleanup of expired items
   */
  private startCleanup(intervalMs: number): void {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
    }

    this.cleanupInterval = setInterval(() => {
      try {
        this.cleanup();
      } catch (error) {
        logger.error(LogCategory.STORAGE, this.providerType, 'Error during cleanup', {
          error: error instanceof Error ? error.message : String(error)
        });
      }
    }, intervalMs);

    // Don't prevent Node from exiting
    if (this.cleanupInterval.unref) {
      this.cleanupInterval.unref();
    }

    logger.debug(LogCategory.STORAGE, this.providerType, 'Started cleanup interval', {
      intervalMs
    });
  }

  /**
   * Clean up expired items
   */
  private cleanup(): void {
    const now = Date.now();
    const stmt = this.db.prepare(`
      DELETE FROM key_value_store 
      WHERE expires_at IS NOT NULL AND expires_at <= ?
    `);

    const result = stmt.run(now);
    
    if (result.changes > 0) {
      logger.debug(LogCategory.STORAGE, this.providerType, 'Cleaned up expired items', {
        count: result.changes
      });
    }
  }

  /**
   * Check if an item has expired
   */
  private isExpired(expiresAt: number | null): boolean {
    return expiresAt !== null && expiresAt <= Date.now();
  }

  // --- StorageProvider Interface Implementation ---

  async get<T>(key: string, options: StorageOptions = {}): Promise<T | null> {
    const namespacedKey = this.getNamespacedKey(key, options.namespace);
    
    try {
      const stmt = this.db.prepare(`
        SELECT value, expires_at 
        FROM key_value_store 
        WHERE key = ?
      `);
      
      const row = stmt.get(namespacedKey) as { value: string; expires_at: number | null } | undefined;
      
      if (!row) {
        return null;
      }

      // Check if expired
      if (this.isExpired(row.expires_at)) {
        // Remove expired item
        await this.delete(key, options);
        return null;
      }

      // Parse and return value
      return JSON.parse(row.value) as T;
    } catch (error) {
      logger.error(LogCategory.STORAGE, this.providerType, 'Error getting value', {
        key: namespacedKey,
        error: error instanceof Error ? error.message : String(error)
      });
      return null;
    }
  }

  async set<T>(key: string, value: T, options: StorageOptions = {}): Promise<void> {
    const namespacedKey = this.getNamespacedKey(key, options.namespace);
    
    try {
      // Calculate expiration time if TTL is provided
      let expiresAt: number | null = null;
      if (typeof options.ttlSeconds === 'number' && options.ttlSeconds > 0) {
        expiresAt = Date.now() + (options.ttlSeconds * 1000);
      }

      // Serialize value and metadata
      const serializedValue = JSON.stringify(value);
      const serializedMetadata = options.metadata ? JSON.stringify(options.metadata) : null;

      const stmt = this.db.prepare(`
        INSERT OR REPLACE INTO key_value_store (key, value, expires_at, metadata)
        VALUES (?, ?, ?, ?)
      `);

      stmt.run(namespacedKey, serializedValue, expiresAt, serializedMetadata);

      logger.debug(LogCategory.STORAGE, this.providerType, 'Set value', {
        key: namespacedKey,
        hasExpiration: expiresAt !== null,
        hasMetadata: serializedMetadata !== null
      });
    } catch (error) {
      logger.error(LogCategory.STORAGE, this.providerType, 'Error setting value', {
        key: namespacedKey,
        error: error instanceof Error ? error.message : String(error)
      });
      throw error;
    }
  }

  async delete(key: string, options: StorageOptions = {}): Promise<boolean> {
    const namespacedKey = this.getNamespacedKey(key, options.namespace);
    
    try {
      const stmt = this.db.prepare(`
        DELETE FROM key_value_store WHERE key = ?
      `);
      
      const result = stmt.run(namespacedKey);
      
      logger.debug(LogCategory.STORAGE, this.providerType, 'Deleted key', {
        key: namespacedKey,
        existed: result.changes > 0
      });
      
      return result.changes > 0;
    } catch (error) {
      logger.error(LogCategory.STORAGE, this.providerType, 'Error deleting key', {
        key: namespacedKey,
        error: error instanceof Error ? error.message : String(error)
      });
      return false;
    }
  }

  async exists(key: string, options: StorageOptions = {}): Promise<boolean> {
    const namespacedKey = this.getNamespacedKey(key, options.namespace);
    
    try {
      const stmt = this.db.prepare(`
        SELECT 1 FROM key_value_store 
        WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)
      `);
      
      const row = stmt.get(namespacedKey, Date.now());
      return row !== undefined;
    } catch (error) {
      logger.error(LogCategory.STORAGE, this.providerType, 'Error checking key existence', {
        key: namespacedKey,
        error: error instanceof Error ? error.message : String(error)
      });
      return false;
    }
  }

  async getMany<T>(keys: string[], options: StorageOptions = {}): Promise<Record<string, T | null>> {
    const result: Record<string, T | null> = {};
    
    for (const key of keys) {
      result[key] = await this.get<T>(key, options);
    }
    
    return result;
  }

  async setMany<T>(items: Record<string, T>, options: StorageOptions = {}): Promise<void> {
    // Use a transaction for better performance
    const transaction = this.db.transaction(() => {
      for (const [key, value] of Object.entries(items)) {
        const namespacedKey = this.getNamespacedKey(key, options.namespace);
        
        // Calculate expiration time if TTL is provided
        let expiresAt: number | null = null;
        if (typeof options.ttlSeconds === 'number' && options.ttlSeconds > 0) {
          expiresAt = Date.now() + (options.ttlSeconds * 1000);
        }

        // Serialize value and metadata
        const serializedValue = JSON.stringify(value);
        const serializedMetadata = options.metadata ? JSON.stringify(options.metadata) : null;

        const stmt = this.db.prepare(`
          INSERT OR REPLACE INTO key_value_store (key, value, expires_at, metadata)
          VALUES (?, ?, ?, ?)
        `);

        stmt.run(namespacedKey, serializedValue, expiresAt, serializedMetadata);
      }
    });

    try {
      transaction();
    } catch (error) {
      logger.error(LogCategory.STORAGE, this.providerType, 'Error setting multiple values', {
        keyCount: Object.keys(items).length,
        error: error instanceof Error ? error.message : String(error)
      });
      throw error;
    }
  }

  async deleteMany(keys: string[], options: StorageOptions = {}): Promise<number> {
    let deletedCount = 0;
    
    const transaction = this.db.transaction(() => {
      for (const key of keys) {
        const namespacedKey = this.getNamespacedKey(key, options.namespace);
        const stmt = this.db.prepare('DELETE FROM key_value_store WHERE key = ?');
        const result = stmt.run(namespacedKey);
        if (result.changes > 0) {
          deletedCount++;
        }
      }
    });

    try {
      transaction();
      return deletedCount;
    } catch (error) {
      logger.error(LogCategory.STORAGE, this.providerType, 'Error deleting multiple keys', {
        keyCount: keys.length,
        error: error instanceof Error ? error.message : String(error)
      });
      throw error;
    }
  }

  async list(prefix: string = '', options: ListOptions = {}): Promise<string[]> {
    const namespacePrefix = options.namespace || this.namespace;
    const searchPattern = `${namespacePrefix}:${prefix}%`;
    
    try {
      const stmt = this.db.prepare(`
        SELECT key FROM key_value_store 
        WHERE key LIKE ? AND (expires_at IS NULL OR expires_at > ?)
        ORDER BY key
        ${options.limit ? `LIMIT ${options.limit}` : ''}
        ${options.offset ? `OFFSET ${options.offset}` : ''}
      `);
      
      const rows = stmt.all(searchPattern, Date.now()) as { key: string }[];
      
      // Remove namespace prefix from returned keys
      const namespacePrefixLength = `${namespacePrefix}:`.length;
      return rows.map(row => row.key.substring(namespacePrefixLength));
    } catch (error) {
      logger.error(LogCategory.STORAGE, this.providerType, 'Error listing keys', {
        prefix: searchPattern,
        error: error instanceof Error ? error.message : String(error)
      });
      return [];
    }
  }

  async clear(prefix?: string, options: StorageOptions = {}): Promise<void> {
    const namespacePrefix = options.namespace || this.namespace;
    
    try {
      let sql: string;
      let params: any[];
      
      if (prefix) {
        const searchPattern = `${namespacePrefix}:${prefix}%`;
        sql = 'DELETE FROM key_value_store WHERE key LIKE ?';
        params = [searchPattern];
      } else {
        const namespacePattern = `${namespacePrefix}:%`;
        sql = 'DELETE FROM key_value_store WHERE key LIKE ?';
        params = [namespacePattern];
      }
      
      const stmt = this.db.prepare(sql);
      const result = stmt.run(...params);
      
      logger.debug(LogCategory.STORAGE, this.providerType, 'Cleared keys', {
        prefix,
        namespace: namespacePrefix,
        deletedCount: result.changes
      });
    } catch (error) {
      logger.error(LogCategory.STORAGE, this.providerType, 'Error clearing keys', {
        prefix,
        namespace: namespacePrefix,
        error: error instanceof Error ? error.message : String(error)
      });
      throw error;
    }
  }

  // --- List Operations ---

  async getList<T>(
    key: string,
    start: number = 0,
    end: number = -1,
    options: StorageOptions = {}
  ): Promise<T[] | null> {
    const value = await this.get<T[]>(key, options);
    
    if (!Array.isArray(value)) {
      return null;
    }

    // Handle Python-style negative indexing for end
    const actualEnd = end < 0 ? value.length + end + 1 : end + 1;
    
    // Slice the array, ensuring indices are within bounds
    const startIndex = Math.max(0, start);
    const endIndex = Math.min(value.length, actualEnd);
    
    if (startIndex >= endIndex) {
      return [];
    }

    return value.slice(startIndex, endIndex);
  }

  async saveList<T>(key: string, values: T[], options: StorageOptions = {}): Promise<void> {
    await this.set(key, values, options);
  }

  async deleteList(key: string, options: StorageOptions = {}): Promise<boolean> {
    return this.delete(key, options);
  }

  // --- Resource Management ---

  async destroy(): Promise<void> {
    try {
      // Stop cleanup interval
      if (this.cleanupInterval) {
        clearInterval(this.cleanupInterval);
        this.cleanupInterval = undefined;
      }

      // Close database connection
      this.db.close();
      
      logger.info(LogCategory.STORAGE, this.providerType, 'SQLite storage provider destroyed');
    } catch (error) {
      logger.error(LogCategory.STORAGE, this.providerType, 'Error destroying provider', {
        error: error instanceof Error ? error.message : String(error)
      });
      throw error;
    }
  }
}
