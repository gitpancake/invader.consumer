import { writeFileSync, appendFileSync, existsSync } from "fs";
import { join } from "path";

export interface IPFSRecord {
  flash_id: number;
  cid: string;
  filename: string;
  ipfs_url: string;
  file_size: number;
  content_type: string;
  uploaded_at: string;
  source: 'S3' | 'API';
}

const CSV_FILE_PATH = join(process.cwd(), 'ipfs-uploads.csv');
const CSV_HEADERS = 'flash_id,cid,filename,ipfs_url,file_size,content_type,uploaded_at,source\n';

export class CSVLogger {
  
  static initializeCSV(): void {
    if (!existsSync(CSV_FILE_PATH)) {
      writeFileSync(CSV_FILE_PATH, CSV_HEADERS, 'utf8');
    }
  }
  
  static logIPFSUpload(record: IPFSRecord): void {
    this.initializeCSV();
    
    const csvRow = [
      record.flash_id,
      record.cid,
      `"${record.filename}"`, // Quote filename in case it contains commas
      record.ipfs_url,
      record.file_size,
      record.content_type,
      record.uploaded_at,
      record.source
    ].join(',') + '\n';
    
    appendFileSync(CSV_FILE_PATH, csvRow, 'utf8');
  }
  
  static getCSVPath(): string {
    return CSV_FILE_PATH;
  }
  
  // Generate Web3.Storage compatible format
  static generateWeb3StorageCSV(): string {
    const web3StoragePath = join(process.cwd(), 'web3-storage-import.csv');
    
    if (!existsSync(CSV_FILE_PATH)) {
      throw new Error('No IPFS uploads CSV found. Upload some files first.');
    }
    
    // Read existing CSV and convert to Web3.Storage format
    const content = require('fs').readFileSync(CSV_FILE_PATH, 'utf8');
    const lines = content.split('\n').slice(1); // Skip header
    
    // Web3.Storage format: tokenID,cid (for general networks)
    let web3Content = 'tokenID,cid\n';
    
    lines.forEach((line: string) => {
      if (line.trim()) {
        const [flash_id, cid] = line.split(',');
        web3Content += `${flash_id},${cid}\n`;
      }
    });
    
    writeFileSync(web3StoragePath, web3Content, 'utf8');
    
    return web3StoragePath;
  }
}