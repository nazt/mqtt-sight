#!/usr/bin/env bun
// MQTT Sight - An advanced MQTT message visualization tool

import * as mqtt from 'mqtt';
import { parseArgs } from 'util';
import { table } from 'table';
import type { TableUserConfig } from 'table';

function showHelp() {
  console.log(`
MQTT Sight - An Advanced MQTT Message Visualizer

Usage: bunx mqtt-sight [options]

Examples:
  bunx mqtt-sight -t "#" -h localhost -u username -P password -d
  bunx mqtt-sight -t "sensor/#" -h localhost -u username -P password --clear
  bunx mqtt-sight -t "#" -h localhost -u username -P password --live
  bunx mqtt-sight -t "#" -h localhost -e "internal/*,debug/*,sys*"
  bunx mqtt-sight -t "#" -h localhost -s topic
  bunx mqtt-sight -t "#" -h localhost -f "error-*,warning-*" --live
  bunx mqtt-sight -t "#" -h localhost -x "password,token,apikey" -p "last4"

Options:
  -t <topic>     Topic to subscribe to (default: "#")
  -h <host>      MQTT broker host (default: "localhost")
  -u <username>  Username for authentication
  -P <password>  Password for authentication (requires -u)
  -d             Enable debug output
  -e, --exclude  Exclude topics matching pattern(s) (comma-separated, supports wildcards)
                 Examples: "temp/*" excludes all temp/ topics, "*log" excludes topics ending with "log"
  -f, --filter   Only include topics/payloads matching pattern(s) (comma-separated, supports wildcards)
                 Examples: "NC-*" only includes topics/payloads containing NC-, "Error-*" includes errors
  -m, --mode     Filter mode: 'topic' (match against topic), 'payload' (match against payload),
                 or 'both' (default: 'both'). Used with -f/--filter to determine where to apply filters
  -x, --mask     Mask patterns in topics and payloads (comma-separated)
                 Examples: "password,token,apikey" will mask these terms
  -p, --preserve Preserve part of masked text: 'none' (mask all), 'first4' (keep first 4 chars),
                 'last4' (keep last 4 chars), or 'both4' (keep first and last 4)
                 Example: with "last4" and mask "password", shows "****word"
  -s, --sort     Sort messages by: 'time' or 'topic' (default: 'time')
  --clear        Clear retained messages on subscribed topics
  --live         Show all messages, not just retained ones (default: show only retained)
  --help         Display this help message

Interactive Commands:
  i              Show detailed information about the most recent message
  +              Increase table width (makes more room for topics)
  -              Decrease table width
  r              Force refresh the display immediately
  a              Toggle auto-refresh mode (default: ON)
  s              Toggle sort mode (time vs topic)
  1              Set sort to topic mode
  2              Set sort to time mode
  f              Toggle filter highlight mode (highlights matching patterns)
  m              Toggle mask mode (on/off)
  any key        Return to table view
  Ctrl+C         Exit the application
  `);
  process.exit(0);
}

// Default column widths
let topicWidth = 60;
let payloadWidth = 40;

// Function to generate table config with current widths
const getTableConfig = (): TableUserConfig => ({
  columns: {
    0: { width: 10, alignment: 'right' as const },      // Row number column - increased width for markers
    1: { width: topicWidth, wrapWord: true, truncate: topicWidth }, // Topic column - adjustable width
    2: { width: payloadWidth, wrapWord: true, truncate: payloadWidth }, // Payload column - adjustable width
    3: { width: 10, alignment: 'center' as const },     // Retain column - fixed width
    4: { width: 15, alignment: 'center' as const },     // Action column - increased width for markers
    5: { width: 20, alignment: 'right' as const }       // Arrival time column - fixed width
  },
  border: {
    topBody: '─',
    topJoin: '┬',
    topLeft: '┌',
    topRight: '┐',
    bottomBody: '─',
    bottomJoin: '┴',
    bottomLeft: '└',
    bottomRight: '┘',
    bodyLeft: '│',
    bodyRight: '│',
    bodyJoin: '│',
    joinBody: '─',
    joinLeft: '├',
    joinRight: '┤',
    joinJoin: '┼'
  }
});

const { values, positionals } = parseArgs({
  options: {
    t: { type: 'string', short: 't', default: '#' },
    h: { type: 'string', short: 'h', default: 'localhost' },
    u: { type: 'string', short: 'u' },
    P: { type: 'string', short: 'P' },
    d: { type: 'boolean', short: 'd', default: false },
    clear: { type: 'boolean', default: false, description: 'Clear retained messages' },
    live: { type: 'boolean', default: false, description: 'Show all messages (not just retained)' },
    exclude: { type: 'string', short: 'e', description: 'Exclude topics matching pattern (comma separated)' },
    filter: { type: 'string', short: 'f', description: 'Include only topics/payloads matching pattern (comma separated)' },
    mode: { type: 'string', short: 'm', default: 'both', description: 'Filter mode: topic, payload, or both' },
    mask: { type: 'string', short: 'x', description: 'Mask patterns in topics and payloads (comma separated)' },
    preserve: { type: 'string', short: 'p', default: 'none', description: 'Preserve part of masked text: none, first4, last4, or both4' },
    sort: { type: 'string', short: 's', default: 'time', description: 'Sort by: time or topic' },
    help: { type: 'boolean', default: false }
  },
  allowPositionals: true
});

// Show help for any of these conditions:
// 1. --help flag is used
// 2. Unknown positional arguments are provided
// 3. Invalid combination of required parameters
if (values.help || positionals.length > 0) {
  showHelp();
}

// Validate required parameters
if (values.P && !values.u) {
  console.error("Error: Password (-P) provided without username (-u)");
  showHelp();
}

const host = values.h;
const topic = values.t;
const username = values.u;
const password = values.P;
const debug = values.d;
const clearRetained = values.clear;
const showLiveMessages = values.live;
const sortOption = values.sort.toLowerCase();

// Validate sort option
if (sortOption !== 'time' && sortOption !== 'topic') {
  console.error(`Invalid sort option: ${sortOption}. Must be 'time' or 'topic'.`);
  showHelp();
}

// Validate filter mode
const filterMode = values.mode.toLowerCase();
if (filterMode !== 'topic' && filterMode !== 'payload' && filterMode !== 'both') {
  console.error(`Invalid filter mode: ${filterMode}. Must be 'topic', 'payload', or 'both'.`);
  showHelp();
}

// Validate preserve mode
const preserveMode = values.preserve.toLowerCase();
if (preserveMode !== 'none' && preserveMode !== 'first4' && preserveMode !== 'last4' && preserveMode !== 'both4') {
  console.error(`Invalid preserve mode: ${preserveMode}. Must be 'none', 'first4', 'last4', or 'both4'.`);
  showHelp();
}

// Process exclude patterns
const excludePatterns: string[] = [];
if (values.exclude) {
  // Split by comma and trim each pattern
  excludePatterns.push(...values.exclude.split(',').map(p => p.trim()));
  if (debug) {
    console.log(`Excluding topics matching patterns: ${excludePatterns.join(', ')}`);
  }
}

// Process filter patterns
const includePatterns: string[] = [];
if (values.filter) {
  // Split by comma and trim each pattern
  includePatterns.push(...values.filter.split(',').map(p => p.trim()));
  if (debug) {
    console.log(`Including only topics/payloads matching patterns: ${includePatterns.join(', ')} (using mode: ${values.mode})`);
  }
}

// Process mask patterns
const maskPatterns: string[] = [];
if (values.mask) {
  // Split by comma and trim each pattern
  maskPatterns.push(...values.mask.split(',').map(p => p.trim()));
  if (debug) {
    console.log(`Masking patterns: ${maskPatterns.join(', ')}`);
    console.log(`Preserve mode: ${preserveMode}`);
  }
}

// Option to highlight matches (can be toggled with 'f' key)
let highlightMatchesEnabled = true;

// Option to enable masking (can be toggled with 'm' key)
let maskingEnabled = maskPatterns.length > 0;

// Helper function to mask sensitive text
const applyMasking = (text: string): string => {
  if (!maskingEnabled || maskPatterns.length === 0) return text;
  
  let maskedText = text;
  
  maskPatterns.forEach(pattern => {
    // Create a regex to find the pattern - case insensitive
    const regex = new RegExp(pattern, 'gi');
    
    // Replace all occurrences with masked version
    maskedText = maskedText.replace(regex, match => {
      // Determine how to mask based on preserveMode
      let masked = '';
      
      switch (preserveMode) {
        case 'none':
          // Replace all characters with asterisks
          masked = '*'.repeat(match.length);
          break;
          
        case 'first4':
          // Keep first 4 characters, mask the rest
          if (match.length <= 4) {
            masked = match; // Too short to mask
          } else {
            masked = match.substring(0, 4) + '*'.repeat(match.length - 4);
          }
          break;
          
        case 'last4':
          // Keep last 4 characters, mask the rest
          if (match.length <= 4) {
            masked = match; // Too short to mask
          } else {
            masked = '*'.repeat(match.length - 4) + match.substring(match.length - 4);
          }
          break;
          
        case 'both4':
          // Keep first and last 4 characters, mask the middle
          if (match.length <= 8) {
            masked = match; // Too short to mask effectively
          } else {
            masked = match.substring(0, 4) + '*'.repeat(match.length - 8) + match.substring(match.length - 4);
          }
          break;
          
        default:
          // Default to masking everything
          masked = '*'.repeat(match.length);
      }
      
      return masked;
    });
  });
  
  return maskedText;
};

// Helper function to check if a topic should be excluded
const shouldExcludeTopic = (topic: string): boolean => {
  if (excludePatterns.length === 0) return false;
  
  return excludePatterns.some(pattern => {
    // Check exact match
    if (topic === pattern) return true;
    
    // Check wildcard match (simple pattern with * at the end)
    if (pattern.endsWith('*') && topic.startsWith(pattern.slice(0, -1))) return true;
    
    // Check wildcard match (simple pattern with * at the beginning)
    if (pattern.startsWith('*') && topic.endsWith(pattern.slice(1))) return true;
    
    return false;
  });
};

// Helper function to check if a topic or payload should be included
// Returns an object with match information or false if no match
const shouldInclude = (topic: string, payload: string): false | { 
  matched: boolean; 
  topicMatched: boolean; 
  payloadMatched: boolean; 
  pattern: string; 
} => {
  // If no include patterns specified, include everything
  if (includePatterns.length === 0) return {
    matched: true,
    topicMatched: false,
    payloadMatched: false,
    pattern: ''
  };
  
  for (const pattern of includePatterns) {
    let topicMatched = false;
    let payloadMatched = false;
    
    // Check topic match if applicable
    if (filterMode === 'topic' || filterMode === 'both') {
      // Check exact match
      if (topic === pattern) {
        topicMatched = true;
      }
      // Check wildcard match (simple pattern with * at the end)
      else if (pattern.endsWith('*') && topic.startsWith(pattern.slice(0, -1))) {
        topicMatched = true;
      }
      // Check wildcard match (simple pattern with * at the beginning)
      else if (pattern.startsWith('*') && topic.endsWith(pattern.slice(1))) {
        topicMatched = true;
      }
      // Check partial match (contains pattern)
      else if (pattern.includes('*')) {
        const regex = new RegExp(pattern.replace(/\*/g, '.*'));
        if (regex.test(topic)) {
          topicMatched = true;
        }
      } else if (topic.includes(pattern)) {
        topicMatched = true;
      }
    }
    
    // Check payload match if applicable
    if (filterMode === 'payload' || filterMode === 'both') {
      // Check exact match
      if (payload === pattern) {
        payloadMatched = true;
      }
      // Check partial match (contains pattern)
      else if (pattern.includes('*')) {
        const regex = new RegExp(pattern.replace(/\*/g, '.*'));
        if (regex.test(payload)) {
          payloadMatched = true;
        }
      } else if (payload.includes(pattern)) {
        payloadMatched = true;
      }
    }
    
    // If any criteria matched, return the match info
    if (topicMatched || payloadMatched) {
      return {
        matched: true,
        topicMatched,
        payloadMatched,
        pattern
      };
    }
  }
  
  // No match found
  return false;
};

if (debug) {
  console.log(`Connecting to ${host} with topic ${topic}`);
  if (username) console.log(`Username: ${username}`);
  if (clearRetained) console.log('Clear retained message mode enabled');
  console.log(`Mode: ${showLiveMessages ? 'Live (showing all messages)' : 'Retained only (showing only retained messages)'}`);
  if (excludePatterns.length > 0) {
    console.log(`Excluding topics matching: ${excludePatterns.join(', ')}`);
  }
}

const connectOptions: mqtt.IClientOptions = {
  clean: true,
  connectTimeout: 4000,
  clientId: `bunx_mqtt_${Math.random().toString(16).substring(2, 8)}`,
};

if (username && password) {
  connectOptions.username = username;
  connectOptions.password = password;
}

// Check for old flag usage
if (process.argv.includes('-i')) {
  console.error("Error: The -i flag has been replaced with -f (filter). Please update your command.");
  showHelp();
}

if (process.argv.includes('-m') && !process.argv.includes('--mode')) {
  // Only error if it's being used as a short flag for mask, not for mode
  const mIndex = process.argv.indexOf('-m');
  const nextArg = process.argv[mIndex + 1];
  
  if (nextArg && !nextArg.match(/^(topic|payload|both)$/i)) {
    console.error("Error: The -m flag for masking has been replaced with -x. Please update your command.");
    showHelp();
  }
}

try {
  if (debug) console.log(`Attempting to connect to ${host}...`);
  
  const client = mqtt.connect(`mqtt://${host}`, connectOptions);
  
  // Set connection timeout
  const connectionTimeout = setTimeout(() => {
    console.error(`Connection timeout: Unable to connect to ${host}`);
    client.end();
    process.exit(1);
  }, 10000); // 10 seconds timeout
  
  client.on('connect', () => {
    clearTimeout(connectionTimeout);
    if (debug) console.log('Connected to MQTT broker');
    
    // Display a connection banner
    const connectionInfo = `Connected to MQTT broker: ${host}`;
    const topicInfo = `Subscribed to topic: ${topic}`;
    const modeInfo = `Mode: ${showLiveMessages ? '🔄 Live (all messages)' : '📌 Retained only (filtered)'}`;
    
    // Add lines to the banner based on configuration
    const bannerLines = [connectionInfo, topicInfo, modeInfo];
    
    // Add exclusion info if applicable
    if (excludePatterns.length > 0) {
      bannerLines.push(`Excluding topics: ${excludePatterns.join(', ')}`);
    }
    
    // Add filter info if applicable
    if (includePatterns.length > 0) {
      bannerLines.push(`Filtering messages matching: ${includePatterns.join(', ')} (mode: ${filterMode})`);
    }
    
    // Add masking info if applicable
    if (maskPatterns.length > 0) {
      const mode = maskingEnabled ? "ON" : "OFF";
      bannerLines.push(`Masking: ${mode} - Patterns: ${maskPatterns.join(', ')} (preserve: ${preserveMode})`);
    }
    
    // Calculate maximum line length for border
    const maxLineLength = Math.max(...bannerLines.map(line => line.length));
    const border = '═'.repeat(maxLineLength + 6);
    
    process.stdout.write('\u001b[1;32m'); // Bright green text
    console.log(`╔${border}╗`);
    
    // Display each line with proper padding
    bannerLines.forEach(line => {
      console.log(`║   ${line}${' '.repeat(Math.max(0, maxLineLength - line.length + 3))}║`);
    });
    
    console.log(`╚${border}╝`);
    process.stdout.write('\u001b[0m'); // Reset text formatting
    
    client.subscribe(topic, (err) => {
      if (err) {
        console.error(`Error subscribing to ${topic}:`, err);
        client.end();
        process.exit(1);
      }
      
      if (debug) console.log(`Subscribed to ${topic}`);
    });
    
    // Start the redraw timer
    startRedrawTimer();
  });

  // Setup terminal for smooth scrolling updates
  // Clear screen and hide cursor
  process.stdout.write('\u001b[2J'); // Clear entire screen
  process.stdout.write('\u001b[H');  // Move cursor to home position
  process.stdout.write('\u001b[?25l'); // Hide cursor
  
  // Make sure cursor is visible again when program exits
  const restoreCursor = () => {
    process.stdout.write('\u001b[?25h'); // Show cursor
  };
  
  // Add cursor restoration on exit
  process.on('exit', restoreCursor);
  
  // Initialize table with headers (sort indicators will be added during redraw)
  console.log(table([
    ['#', 'Topic', 'Payload', 'Retained', 'Action', 'Arrival Time']
  ], getTableConfig()));
  
  // Show exclude patterns if any are set
  if (excludePatterns.length > 0) {
    console.log(`\u001b[1;33mFiltering: Excluding topics matching patterns: ${excludePatterns.join(', ')}\u001b[0m`);
  }

  // Store received messages
  const messages: Record<string, { 
    payload: string, 
    retained: boolean, 
    timestamp: number, 
    cleared?: boolean,
    // Add a flag to track if this message contains ANSI color codes
    hasColorCodes?: boolean,
    // Track if this message matches include patterns and which pattern it matched
    matchInfo?: {
      matched: boolean,
      topicMatched: boolean,
      payloadMatched: boolean,
      pattern: string
    }
  }> = {};
  
  // Message processing queue to prevent UI freeze
  let processingMessages = false;
  const messageQueue: Array<{
    topic: string, 
    payload: string, 
    retained: boolean,
    matchInfo?: {
      matched: boolean;
      topicMatched: boolean;
      payloadMatched: boolean;
      pattern: string;
    }
  }> = [];

  // Function to clear a retained message
  const clearRetainedMessage = (topicToClear: string) => {
    if (debug) console.log(`Clearing retained message for topic: ${topicToClear}`);
    
    // Publishing an empty message with retained=true clears the retained message
    client.publish(topicToClear, '', { retain: true, qos: 1 }, (err) => {
      if (err) {
        console.error(`Error clearing retained message for ${topicToClear}:`, err);
      } else if (debug) {
        console.log(`Successfully cleared retained message for ${topicToClear}`);
      }
      
      // Mark as cleared in our local state
      if (messages[topicToClear]) {
        messages[topicToClear].cleared = true;
        
        // Update the queue status line immediately without full refresh
        updateStatusLine();
        
        // Then do a full redraw if needed
        redrawTable();
      }
    });
  };

  // Variables to control table redraw
  let redrawRequested = false;
  let lastRedrawTime = 0;
  let redrawInterval = 1000; // 1 second interval by default
  const longRedrawInterval = 15000; // 15 seconds interval for large datasets
  let redrawTimer: ReturnType<typeof setInterval> | null = null;
  let autoRefresh = true; // Default is auto-refresh mode (enabled)
  let countdownTimer: ReturnType<typeof setInterval> | null = null;
  let countdownValue = 0;
  
  // Set to true to show all messages, not just the most recent 100
  const showAllMessages = true;
  
  // Track the last message count for marking new messages
  let lastMessageCount = 0;
  
  // Track the terminal size for fixed footer
  const getTerminalSize = () => ({
    rows: process.stdout.rows || 30,
    columns: process.stdout.columns || 100
  });

  // Function to redraw the table
  const redrawTable = () => {
    const currentTime = Date.now();
    
    // If we're not due for a redraw yet and auto-refresh is enabled, just mark as requested and return
    if (autoRefresh && currentTime - lastRedrawTime < redrawInterval) {
      redrawRequested = true;
      return;
    }
    
    // Update without clearing the entire console (scroll effect)
    // Save cursor position and clear from cursor to end of screen
    process.stdout.write('\u001b[s'); // Save cursor position
    process.stdout.write('\u001b[0J'); // Clear from cursor to end of screen
    
    // Move cursor to top of content area
    process.stdout.write('\u001b[H'); // Move cursor to home position
    
    // Reset any ANSI colors that might have leaked from previous output
    process.stdout.write('\u001b[0m'); // Reset all formatting
    
    lastRedrawTime = currentTime;
    redrawRequested = false;
    
    // Convert messages to table data with sort indicators in headers
    const tableData = [
      [
        '#', 
        // Add sort indicator and highlight to Topic header when sorting by topic
        sortOption === 'topic' ? '\u001b[1;33mTopic 📂\u001b[0m' : 'Topic',
        'Payload', 
        'Retained', 
        'Action', 
        // Add sort indicator and highlight to Time header when sorting by time
        sortOption === 'time' ? '\u001b[1;33mArrival Time 🕒\u001b[0m' : 'Arrival Time'
      ]
    ];
    
    // Get total message count
    const totalMessages = Object.keys(messages).length;
    
    // Adjust redraw interval based on message count
    if (totalMessages > 1000 && redrawInterval !== longRedrawInterval) {
      // Switch to longer interval for large datasets
      redrawInterval = longRedrawInterval;
      
      // Update countdown timer immediately
      countdownValue = Math.ceil(redrawInterval / 1000);
      
      // If we're switching to slow mode, force an immediate update to show all messages
      if (redrawTimer) {
        clearInterval(redrawTimer);
        redrawTimer = setInterval(() => {
          if (redrawRequested && autoRefresh) {
            redrawTable();
          }
        }, redrawInterval);
      }
    }
    
    // Default max displayed or show all based on setting
    const maxDisplayedMessages = showAllMessages ? Number.MAX_SAFE_INTEGER : 100;
    
    // Get sorted messages based on specified sort option
    const sortedMessages = Object.entries(messages)
      .sort((a, b) => {
        if (sortOption === 'time') {
          // Sort by timestamp - oldest first (oldest timestamp at top)
          return a[1].timestamp - b[1].timestamp;
        } else if (sortOption === 'topic') {
          // Sort by topic - alphabetically
          return a[0].localeCompare(b[0], undefined, { sensitivity: 'base' });
        }
        // Default to timestamp sort
        return a[1].timestamp - b[1].timestamp;
      });
      
    // Apply limit only if we're not showing all messages
    const limitedMessages = showAllMessages ? 
      sortedMessages : 
      sortedMessages.slice(0, maxDisplayedMessages);
      
    // Add messages with row numbers - oldest is #1 (at top) 
    limitedMessages.forEach(([topic, data], index) => {
        let actionStatus = '';
        
        if (data.cleared) {
          actionStatus = '🧹 Cleared';
        } else if (data.retained && clearRetained) {
          actionStatus = '🔄 Will clear';
        }
        
        // Truncate topic and payload to fit within table constraints
        let displayTopic = topic;
        let displayPayload = data.payload;
        
        // Function to strip ANSI color codes from text
        const stripAnsiCodes = (text: string): string => {
          return text.replace(/\u001b\[\d+(;\d+)*m/g, '');
        };
        
        // Check if payload has ANSI color codes
        const originalPayload = displayPayload;
        const strippedPayload = stripAnsiCodes(displayPayload);
        const hasColorCodes = originalPayload !== strippedPayload;
        
        // Display a notification if this message has color codes
        if (hasColorCodes) {
          actionStatus = data.cleared ? actionStatus : '🎨 Has colors';
          
          // For payloads with color, ensure it ends with a reset code
          if (!displayPayload.endsWith('\u001b[0m')) {
            displayPayload = displayPayload + '\u001b[0m';
          }
        } else {
          // Use the stripped version when there are no intentional color codes
          displayPayload = strippedPayload;
        }
        
        // Truncate payload if it's too long - use shorter truncation for large payloads
        // For payloads with color codes, we need to be careful with truncation
        if (hasColorCodes) {
          // Reset to handle cases where we're truncating in the middle of a color sequence
          const visibleLength = strippedPayload.length;
          if (visibleLength > 1000) {
            // Find a safe truncation point
            const truncPoint = Math.min(100, displayPayload.length);
            displayPayload = displayPayload.substring(0, truncPoint) + 
              '\u001b[0m... [colored, truncated, full length: ' + visibleLength + ' chars]';
          } else if (visibleLength > 500) {
            const truncPoint = Math.min(250, displayPayload.length);
            displayPayload = displayPayload.substring(0, truncPoint) + 
              '\u001b[0m... [colored, truncated]';
          }
        } else {
          // Standard truncation for non-colored payloads
          if (displayPayload.length > 1000) {
            displayPayload = displayPayload.substring(0, 100) + 
              '... [truncated, full length: ' + displayPayload.length + ' chars]';
          } else if (displayPayload.length > 500) {
            displayPayload = displayPayload.substring(0, 250) + '... [truncated]';
          }
        }
        
        // Add timestamp for debugging - convert to seconds ago for easy comparison
        const secondsAgo = Math.floor((Date.now() - data.timestamp) / 1000);
        
        // Row number and optional timing indicator when in auto-refresh mode
        let rowDisplay = `${index + 1}`;
        if (debug) {
          rowDisplay += ` (${secondsAgo}s)`;
        }
        
        // Mark the rows from the previous refresh cycle
        let rowHighlight = "";
        
        // Only apply markers when we have a valid lastMessageCount and we're in auto-refresh
        if (autoRefresh && lastMessageCount > 0 && lastMessageCount <= sortedMessages.length) {
          // Mark the oldest message from the previous batch
          if (index === 0) {
            // Only mark if there were messages in previous refresh
            if (sortedMessages.length > lastMessageCount) {
              rowHighlight = "\u001b[45m"; // Magenta background for first overall
              rowDisplay = rowHighlight + "◉" + rowDisplay + "\u001b[0m"; // Reset formatting after
              actionStatus = "\u001b[45m" + "FIRST MSG" + "\u001b[0m";
            }
          }
          // Mark where the previous refresh started (oldest message in previous refresh)
          else if (sortedMessages.length > lastMessageCount && 
                   index === sortedMessages.length - lastMessageCount) {
            rowHighlight = "\u001b[44m"; // Blue background
            rowDisplay = rowHighlight + "▲" + rowDisplay + "\u001b[0m"; // Reset formatting after
            actionStatus = "\u001b[44m" + "PREV START" + "\u001b[0m";
          }
          // Mark where the previous refresh ended (newest message in previous refresh)
          else if (index === sortedMessages.length - 1 && index < lastMessageCount) {
            rowHighlight = "\u001b[46m"; // Cyan background
            rowDisplay = rowHighlight + "⚓" + rowDisplay + "\u001b[0m"; // Reset formatting after
            actionStatus = "\u001b[46m" + "PREV END" + "\u001b[0m";
          }
          // Mark all messages that are new since last refresh
          else if (sortedMessages.length > lastMessageCount && 
                   index > sortedMessages.length - lastMessageCount) {
            rowHighlight = "\u001b[42m"; // Green background
            rowDisplay = rowHighlight + "✨" + rowDisplay + "\u001b[0m"; // Reset formatting after
          }
        }
        
        // Ensure all ANSI colors in the topic are stripped as well
        displayTopic = stripAnsiCodes(displayTopic);
        
        // Apply masking to topic and payload if enabled
        if (maskingEnabled) {
          displayTopic = applyMasking(displayTopic);
          displayPayload = applyMasking(displayPayload);
        }
        
        // Function to highlight matched patterns in text
        const applyHighlight = (text: string, pattern: string, isMatch: boolean): string => {
          if (!highlightMatchesEnabled || !isMatch || !pattern || pattern === '') return text;
          
          // Convert wildcard pattern to regex pattern
          let regexPattern = pattern.replace(/\*/g, '.*');
          
          try {
            const regex = new RegExp(`(${regexPattern})`, 'gi');
            // Highlight all matches with bright green background
            return text.replace(regex, '\u001b[42;1m$1\u001b[0m');
          } catch (e) {
            // If regex fails, just return original text
            return text;
          }
        };
        
        // Apply highlighting to topic and payload if they matched
        let highlightedTopic = displayTopic;
        let highlightedPayload = displayPayload;
        
        // If message has match info and highlight is enabled, apply highlights
        if (data.matchInfo && highlightMatchesEnabled) {
          if (data.matchInfo.topicMatched) {
            highlightedTopic = applyHighlight(displayTopic, data.matchInfo.pattern, true);
          }
          
          if (data.matchInfo.payloadMatched) {
            // If payload already has color codes, be careful with highlighting
            if (!hasColorCodes) {
              highlightedPayload = applyHighlight(displayPayload, data.matchInfo.pattern, true);
            }
          }
        }
        
        // For payloads with color, we need to wrap them to ensure they don't affect the rest of the table
        const safePayload = hasColorCodes ? 
          displayPayload + '\u001b[0m' : // Ensure color reset after payload  
          highlightedPayload + '\u001b[0m';
          
        // Add reset after every cell to prevent color bleeding
        const retainedStatus = data.retained ? '✅ Yes\u001b[0m' : '❌ No\u001b[0m';
        const safeAction = actionStatus + '\u001b[0m';
        
        // Format arrival time nicely
        const arrivalTime = new Date(data.timestamp).toLocaleTimeString();
        
        tableData.push([
          rowDisplay + '\u001b[0m', // Reset after row number with marker
          highlightedTopic + '\u001b[0m', // Reset after topic
          safePayload, // Already has reset if needed
          retainedStatus,
          safeAction,
          arrivalTime + '\u001b[0m' // Add arrival time with reset
        ]);
      });
    
    // Display the table with current config
    console.log(table(tableData, getTableConfig()) + '\u001b[0m'); // Ensure reset after table
    
    // Get count of NEW rows to mark on next refresh
    const newLastMessageCount = sortedMessages.length;
    
    // Count messages with color codes
    const colorCodedMessages = Object.values(messages).filter(m => m.hasColorCodes).length;
    
    // Display summary stats
    const retainedCount = Object.values(messages).filter(m => m.retained).length;
    const clearedCount = Object.values(messages).filter(m => m.cleared).length;
    const timestamp = new Date().toLocaleTimeString();
    const totalTopics = Object.keys(messages).length;
    
    // Now that we've drawn everything, set this refresh's message count for next time
    lastMessageCount = newLastMessageCount;
    
    // Show whether we're using the slower refresh rate
    const refreshRateMsg = redrawInterval === longRedrawInterval ? 
      `\u001b[1;33mUsing slower refresh rate (${redrawInterval/1000}s) due to large dataset\u001b[0m` : 
      `Standard refresh rate (${redrawInterval/1000}s)`;
    
    // Position cursor at the fixed bottom section of the screen
    // Calculate how many rows we need for the footer
    const termSize = getTerminalSize();
    const footerRows = 9; // Number of lines in our footer
    
    // Position cursor at the fixed footer position
    process.stdout.write(`\u001b[${termSize.rows - footerRows};0H`);
    
    // Clear from cursor to end of screen to ensure footer is clean
    process.stdout.write('\u001b[J');
    
    // Draw fixed footer with divider
    console.log("─".repeat(termSize.columns - 2));
    
    // Show message ordering explanation with visual indication and sort icons
    console.log(`\u001b[1mMessages ordered by\u001b[0m: ${sortOption === 'time' ? 
      `Chronological (\u001b[1;36m#1 = oldest\u001b[0m → \u001b[90m#${sortedMessages.length} = newest\u001b[0m) 🕒` : 
      `Topic name (\u001b[1;36malphabetical A-Z\u001b[0m) 📂` 
    }`);
    
    // Show new message indicator explanation if we're in auto-refresh mode and we have messages
    if (autoRefresh && lastMessageCount > 0) {
      const newMessageCount = Math.max(0, totalMessages - lastMessageCount);
      
      // Create a detailed legend - use shorter text for better table formatting
      const legend = [
        "\u001b[45m◉\u001b[0m First msg",
        "\u001b[44m▲\u001b[0m Prev start",
        "\u001b[46m⚓\u001b[0m Prev end"
      ];
      
      if (newMessageCount > 0) {
        legend.push(`\u001b[42m✨\u001b[0m ${newMessageCount} new messages since last refresh`);
      } else {
        legend.push("No new messages since last refresh");
      }
      
      // Display legend with buffer info
      console.log(legend.join(" | "));
      
      // Show buffer info with warnings
      let bufferMsg = `Buffer: ${messageQueue.length} messages pending`;
      if (messageQueue.length > 100) {
        bufferMsg = `\u001b[1;33mBuffer: ${messageQueue.length} messages pending\u001b[0m`;
      }
      if (messageQueue.length > 500) {
        bufferMsg = `\u001b[1;31mBuffer: ${messageQueue.length} messages pending\u001b[0m`;
      }
      console.log(bufferMsg);
    }
    // Add mode, filtering, and exclusion info
    const modeInfo = showLiveMessages ? "🔄 Live mode" : "📌 Retained-only mode";
    const excludeInfo = excludePatterns.length > 0 ? 
      `| 🚫 Excluding ${excludePatterns.length} pattern${excludePatterns.length > 1 ? 's' : ''}` : 
      '';
    const includeInfo = includePatterns.length > 0 ?
      `| 🔍 Filtering for ${includePatterns.join(', ')} (mode: ${filterMode})` :
      '';
    const maskInfo = maskPatterns.length > 0 ?
      `| 🎭 Masking ${maskingEnabled ? 'ON' : 'OFF'} (${preserveMode})` :
      '';
    const colorInfo = colorCodedMessages > 0 ?
      `| 🎨 ${colorCodedMessages} message${colorCodedMessages > 1 ? 's' : ''} with color codes` :
      '';
    console.log(`${modeInfo} ${excludeInfo} ${includeInfo} ${maskInfo} ${colorInfo} | Last updated: ${timestamp} | Total topics: ${totalTopics} | Retained: ${retainedCount} | Cleared: ${clearedCount}`);
    
    // Show dataset size and refresh info
    if (showAllMessages) {
      console.log(`📊 Showing all ${totalTopics} messages | ${refreshRateMsg}`);
    } else if (totalTopics > maxDisplayedMessages) {
      console.log(`⚠️ Showing oldest ${maxDisplayedMessages} of ${totalTopics} messages to prevent UI freeze. Newest messages may not be visible.`);
    }
    
    // Current time and date
    const now = new Date();
    const timeString = now.toLocaleTimeString();
    const dateString = now.toLocaleDateString();
    
    console.log(`Current time: ${dateString} ${timeString} | Queue: ${messageQueue.length} pending`);
    console.log(`Press 'i' for details | '+' increase width | '-' decrease | 'r' refresh | 'a' toggle auto-refresh (${autoRefresh ? 'ON' : 'OFF'}) | 's' toggle sort ${sortOption === 'time' ? '🕒 time' : '📂 topic'} | '1' sort by topic | '2' sort by time | Ctrl+C exit`);
    
    // Display auto-refresh status with an indicator
    const refreshStatus = autoRefresh ? 
      `\u001b[1;32mAuto-refresh: ON - Next refresh in ${countdownValue}s\u001b[0m` : // Bright green with countdown
      '\u001b[1;33mAuto-refresh: OFF - Press r to refresh manually\u001b[0m';  // Bright yellow
      
    console.log(refreshStatus);
    
    // Save cursor position after drawing the interface
    process.stdout.write('\u001b[s');
  };
  
  // Function to update status line without refreshing the whole screen
  const updateStatusLine = () => {
    // Current queue length with color highlighting when buffer is large
    let queueStatus = `Buffer: ${messageQueue.length} pending`;
    if (messageQueue.length > 50) {
      queueStatus = `\u001b[1;33mBuffer: ${messageQueue.length} pending\u001b[0m`; // Yellow warning
    }
    if (messageQueue.length > 200) {
      queueStatus = `\u001b[1;31mBuffer: ${messageQueue.length} pending\u001b[0m`; // Red warning
    }
    
    // Current time and date
    const now = new Date();
    const timeString = now.toLocaleTimeString();
    const dateString = now.toLocaleDateString();
    const dateTimeStatus = `${dateString} ${timeString}`;
    
    // Countdown display when auto-refresh is on
    let countdownStatus = '';
    if (autoRefresh) {
      countdownStatus = `Next refresh in ${countdownValue}s`;
    }
    
    // Position cursor at the fixed position in footer
    const termSize = getTerminalSize();
    const countdownRow = termSize.rows - 2; // Second to last row in the terminal
    
    // Move cursor to countdown position
    process.stdout.write(`\u001b[${countdownRow};0H`);
    
    // Create a refresh status line with countdown
    const refreshStatus = autoRefresh ? 
      `\u001b[1;32mAuto-refresh: ON - Next refresh in ${countdownValue}s\u001b[0m` : // Bright green with countdown
      '\u001b[1;33mAuto-refresh: OFF - Press r to refresh manually\u001b[0m';  // Bright yellow
    
    // Clear the line and write the new status
    process.stdout.write('\u001b[2K'); // Clear entire line
    process.stdout.write(refreshStatus);
    
    // Also update the time line (one line above)
    process.stdout.write(`\u001b[${countdownRow-1};0H`);
    process.stdout.write('\u001b[2K'); // Clear entire line
    process.stdout.write(`Current time: ${dateString} ${timeString} | ${queueStatus}`);
    
    // Update the buffer line with more details if we have lots of messages pending
    if (messageQueue.length > 0) {
      process.stdout.write(`\u001b[${countdownRow-2};0H`);
      process.stdout.write('\u001b[2K'); // Clear entire line
      
      const processingRate = Math.round(10 * 1000 / redrawInterval); // 10 msgs per redrawInterval ms
      const timeToProcess = Math.ceil(messageQueue.length / processingRate);
      
      process.stdout.write(`Buffer status: Processing ~${processingRate} msgs/sec | Est. time to process all: ${timeToProcess} sec`);
    }
    
    // Restore cursor position
    process.stdout.write('\u001b[u');
  };
  
  // Start the countdown timer
  const startCountdownTimer = () => {
    if (countdownTimer) {
      clearInterval(countdownTimer);
    }
    
    // Set initial countdown value
    countdownValue = Math.ceil(redrawInterval / 1000);
    
    // Update every second
    countdownTimer = setInterval(() => {
      countdownValue--;
      
      // Update the status line without full refresh
      if (autoRefresh) {
        updateStatusLine();
      }
      
      // Reset when we reach zero
      if (countdownValue <= 0) {
        // Use the current redrawInterval in case it's changed
        countdownValue = Math.ceil(redrawInterval / 1000);
      }
    }, 1000);
  };
  
  // Start the interval timer to redraw the table if updates are requested
  const startRedrawTimer = () => {
    if (redrawTimer) return; // Timer already running
    
    // Start countdown
    startCountdownTimer();
    
    redrawTimer = setInterval(() => {
      // In auto-refresh mode, update whenever requested
      if (redrawRequested && autoRefresh) {
        redrawTable();
      }
      
      // Clear excess messages from queue if it gets too large
      // (we don't want memory to grow unbounded)
      if (messageQueue.length > 1000) {
        if (debug) console.log(`Queue overload, clearing ${messageQueue.length - 100} messages`);
        messageQueue.splice(100, messageQueue.length - 100);
      }
    }, redrawInterval);
  };

  // Process message queue asynchronously to prevent UI freeze
  const processMessageQueue = () => {
    if (processingMessages || messageQueue.length === 0) return;
    
    processingMessages = true;
    
    setTimeout(() => {
      // Process a batch of messages (maximum 10 at once)
      const batch = messageQueue.splice(0, Math.min(10, messageQueue.length));
      let needsRedraw = false;
      const now = Date.now();
      
      batch.forEach(({topic, payload, retained, matchInfo}) => {
        // Store or update message
        // Don't update if message content is the same (to reduce unnecessary refreshes)
        if (!messages[topic] || messages[topic].payload !== payload || messages[topic].retained !== retained) {
          // Check if the payload contains ANSI color codes
          const hasColorCodes = payload !== payload.replace(/\u001b\[\d+(;\d+)*m/g, '');
          
          messages[topic] = {
            payload,
            retained,
            timestamp: now, // Use same timestamp for all messages in batch
            hasColorCodes,
            matchInfo // Store the match information
          };
          needsRedraw = true;
          
          // If this is a retained message and we're in clear mode, clear it
          if (clearRetained && retained && payload.length > 0) {
            clearRetainedMessage(topic);
          }
        }
      });
      
      // Only request redraw if something actually changed
      if (needsRedraw) {
        // Request a redraw
        redrawRequested = true;
        
        // Always update the status line immediately to show queue changes
        updateStatusLine();
        
        // For immediate feedback, always update on the first few messages regardless of mode
        // After that, respect the auto-refresh setting
        if (!autoRefresh || Object.keys(messages).length < 10) {
          lastRedrawTime = 0;
          redrawTable();
        }
      }
      
      processingMessages = false;
      
      // Continue processing if there are more messages
      if (messageQueue.length > 0) {
        processMessageQueue();
      }
    }, 10); // Small delay to reduce CPU usage
  };

  client.on('message', (receivedTopic, message, packet) => {
    const isRetained = packet.retain;
    const payload = message.toString();
    
    // Check exclusions first - don't even bother with retention check if excluded
    if (shouldExcludeTopic(receivedTopic)) {
      if (debug) console.log(`Excluded topic: ${receivedTopic}`);
      return;
    }
    
    // Check inclusion patterns - only include messages that match the patterns
    const matchInfo = shouldInclude(receivedTopic, payload);
    if (!matchInfo) {
      if (debug) console.log(`Skipping non-matching message on topic: ${receivedTopic}`);
      return;
    }
    
    // Skip non-retained messages unless in live mode
    if (!isRetained && !showLiveMessages) {
      if (debug) console.log(`Skipping non-retained message on topic: ${receivedTopic}`);
      return;
    }
    
    // Add message to queue instead of processing immediately
    messageQueue.push({
      topic: receivedTopic,
      payload,
      retained: isRetained,
      matchInfo: matchInfo
    });
    
    // Process the queue
    processMessageQueue();
  });
  
  client.on('offline', () => {
    console.error('Disconnected from broker');
  });

  client.on('error', (err) => {
    clearTimeout(connectionTimeout);
    console.error('Connection error:', err);
    client.end();
    process.exit(1);
  });

  // Add handler for viewing full topic details and adjusting table width
  try {
    process.stdin.setRawMode(true);
    process.stdin.resume();
  } catch (error) {
    if (debug) console.log('Warning: Raw mode not supported in this environment. Interactive commands disabled.');
  }
  process.stdin.on('data', (key) => {
    const keyStr = key.toString();
    
    // Check key press
    if (keyStr === 'i') {
      // Info view - this is interactive so always show immediately
      // Clear screen but use proper terminal control codes
      process.stdout.write('\u001b[2J'); // Clear entire screen
      process.stdout.write('\u001b[H');  // Move cursor to home position
      
      const entries = Object.entries(messages)
        .sort((a, b) => b[1].timestamp - a[1].timestamp); // Sort by most recent
      
      if (entries.length > 0) {
        const [topic, data] = entries[0]; // Get most recent message
        
        console.log('\n=== Detailed Topic Information ===');
        console.log(`Full Topic: ${topic}`);
        
        if (data.payload && data.payload.length > 1000) {
          // For large payloads, show only first portion with indicator
          console.log(`Payload (first 1000/${data.payload.length} chars):`);
          console.log(data.payload.substring(0, 1000) + '...');
        } else {
          console.log(`Payload: ${data.payload}`);
        }
        
        console.log(`Retained: ${data.retained ? 'Yes' : 'No'}`);
        
        // Formatted arrival time with date and time
        const formattedDateTime = new Date(data.timestamp).toLocaleString();
        console.log(`Arrival Time: ${formattedDateTime}`);
        
        // Show seconds ago for better relative time understanding
        const secondsAgo = Math.floor((Date.now() - data.timestamp) / 1000);
        if (secondsAgo < 60) {
          console.log(`Received: ${secondsAgo} seconds ago`);
        } else if (secondsAgo < 3600) {
          console.log(`Received: ${Math.floor(secondsAgo / 60)} minutes ago`);
        } else {
          console.log(`Received: ${Math.floor(secondsAgo / 3600)} hours ago`);
        }
        
        if (data.cleared) {
          console.log('Status: Cleared');
        }
        console.log('\nPress any key to return to the table view...');
      } else {
        console.log('No messages received yet.');
        console.log('\nPress any key to return...');
      }
    } else if (keyStr === '+' || keyStr === '=') {
      // Increase table width - user interaction should be responsive
      topicWidth += 5;
      payloadWidth += 5;
      // Force an immediate redraw regardless of timer
      lastRedrawTime = 0;
      redrawTable();
    } else if (keyStr === '-' || keyStr === '_') {
      // Decrease table width, but don't go below minimum
      if (topicWidth > 30) topicWidth -= 5;
      if (payloadWidth > 20) payloadWidth -= 5;
      // Force an immediate redraw regardless of timer
      lastRedrawTime = 0;
      redrawTable();
    } else if (keyStr === '\u0003') {
      // Ctrl+C
      if (debug) console.log('Disconnecting from broker');
      // Clear the interval timer
      if (redrawTimer) {
        clearInterval(redrawTimer);
      }
      client.end();
      process.exit(0);
    } else if (keyStr === 'r') {
      // Manual refresh - force immediate redraw
      lastRedrawTime = 0;
      redrawTable();
    } else if (keyStr === 'a') {
      // Toggle auto-refresh mode
      autoRefresh = !autoRefresh;
      
      // Reset countdown when toggling
      countdownValue = Math.ceil(redrawInterval / 1000);
      
      // Force update
      lastRedrawTime = 0;
      redrawTable();
      
      // Show immediate feedback
      if (autoRefresh) {
        updateStatusLine();
      }
    } else if (keyStr === 's') {
      // Toggle sort mode between time and topic
      const newSortOption = sortOption === 'time' ? 'topic' : 'time';
      
      // Update the sortOption variable
      Object.defineProperty(globalThis, 'sortOption', {
        value: newSortOption,
        writable: true,
        configurable: true
      });
      
      // Force immediate redraw
      lastRedrawTime = 0;
      redrawTable();
    } else if (keyStr === '1') {
      // Set sort mode to topic explicitly
      Object.defineProperty(globalThis, 'sortOption', {
        value: 'topic',
        writable: true,
        configurable: true
      });
      
      // Force immediate redraw
      lastRedrawTime = 0;
      redrawTable();
    } else if (keyStr === '2') {
      // Set sort mode to time explicitly
      Object.defineProperty(globalThis, 'sortOption', {
        value: 'time',
        writable: true,
        configurable: true
      });
      
      // Force immediate redraw
      lastRedrawTime = 0;
      redrawTable();
    } else if (keyStr === 'f') {
      // Toggle highlight mode for matched patterns
      highlightMatchesEnabled = !highlightMatchesEnabled;
      
      // Show a notification about the change
      console.log(`\u001b[1;33mPattern highlighting ${highlightMatchesEnabled ? 'enabled' : 'disabled'}\u001b[0m`);
      
      // Force immediate redraw
      lastRedrawTime = 0;
      redrawTable();
    } else if (keyStr === 'm') {
      // Only toggle if mask patterns are configured
      if (maskPatterns.length > 0) {
        // Toggle masking mode
        maskingEnabled = !maskingEnabled;
        
        // Show a notification about the change
        console.log(`\u001b[1;33mMasking ${maskingEnabled ? 'enabled' : 'disabled'} (${preserveMode} mode)\u001b[0m`);
        
        // Force immediate redraw
        lastRedrawTime = 0;
        redrawTable();
      } else {
        console.log('\u001b[1;33mNo mask patterns configured. Use -m/--mask option to set them.\u001b[0m');
      }
    } else {
      // Any other key - request a table redraw
      // If returning from info view, force immediate redraw
      lastRedrawTime = 0;
      redrawTable();
    }
  });

  // Handle process termination
  process.on('SIGINT', () => {
    if (debug) console.log('Disconnecting from broker');
    
    // Clean up all timers
    if (redrawTimer) {
      clearInterval(redrawTimer);
    }
    
    if (countdownTimer) {
      clearInterval(countdownTimer);
    }
    
    restoreCursor();
    client.end();
    process.exit(0);
  });
} catch (error) {
  if (error instanceof Error) {
    console.error('Error:', error.message);
  } else {
    console.error('Error:', String(error));
  }
  process.exit(1);
}