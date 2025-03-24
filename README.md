# MQTT Sight

An advanced MQTT message visualization tool with powerful filtering, masking, and display options.

## Installation

```bash
# Clone the repository
git clone https://github.com/nazt/mqtt-sight.git
cd mqtt-sight

# Install dependencies
bun install

# You can use this tool with `bunx` without installing it globally
bunx mqtt-sight
```

## Usage

```bash
# Basic usage
bunx mqtt-sight -t "topic" -h "mqtt-broker-host"

# With authentication
bunx mqtt-sight -t "#" -h localhost -u username -P password -d

# Filter for specific patterns
bunx mqtt-sight -t "#" -h localhost -f "error-*,warning-*" --live

# Mask sensitive data (keeping last 4 characters visible)
bunx mqtt-sight -t "#" -h localhost -x "password,apikey,token" -p "last4"

# Clear retained messages
bunx mqtt-sight -t "sensors/#" -h localhost -u username -P password --clear

# Show help
bunx mqtt-sight --help
```

### Command Line Options

- `-t <topic>`: Topic to subscribe to (default: "#")
- `-h <host>`: MQTT broker host (default: "localhost")
- `-u <username>`: Username for authentication
- `-P <password>`: Password for authentication (requires -u)
- `-d`: Enable debug output
- `-e, --exclude`: Exclude topics matching pattern(s)
- `-f, --filter`: Only include topics/payloads matching pattern(s)
- `-m, --mode`: Filter mode: 'topic', 'payload', or 'both' (default)
- `-x, --mask`: Mask patterns in topics and payloads
- `-p, --preserve`: Preserve part of masked text: 'none', 'first4', 'last4', or 'both4'
- `-s, --sort`: Sort messages by: 'time' or 'topic' (default: 'time')
- `--clear`: Clear retained messages on subscribed topics
- `--live`: Show all messages, not just retained ones
- `--help`: Display help message

## Features

- Easy to use MQTT subscriber with advanced visualization
- Flexible filtering with include/exclude patterns
- Topic and payload masking for sensitive information
- Multiple sorting options with visual indicators
- Pattern highlighting and toggling
- Table-based display of messages with color coding
- Retained message identification and management
- Interactive interface with real-time controls
- Support for authentication
- Debug mode for viewing connection details
- Performance optimized for high message rates

## Display Features

The CLI presents MQTT messages in a table with the following columns:

- **Topic**: The MQTT topic (truncated for long topics)
- **Payload**: The message content
- **Retained**: Shows whether the message is retained (✅ Yes/❌ No)
- **Action**: Shows the status of retained message clearing (when `--clear` is used)
- **Arrival Time**: When the message was received

### Interactive Commands

The tool includes interactive keyboard commands:

- Press `i` to show detailed information about the most recent message
- Press `+` to increase the table width (makes more room for long topics)
- Press `-` to decrease the table width 
- Press `r` to force an immediate refresh of the display
- Press `a` to toggle auto-refresh mode
- Press `s` to toggle sort mode (time vs topic)
- Press `1` to set sort to topic mode
- Press `2` to set sort to time mode
- Press `f` to toggle filter highlight mode
- Press `m` to toggle mask mode (on/off)
- Press any key to return to the table view
- Press `Ctrl+C` to exit the application

#### Advanced Filtering

MQTT Sight supports powerful filtering capabilities:

- **Filter Patterns** (`-f, --filter`): Only show messages matching specific patterns
- **Exclude Patterns** (`-e, --exclude`): Hide messages matching specific patterns
- **Filter Modes** (`-m, --mode`): Apply filters to topics, payloads, or both
- **Pattern Highlighting**: Visually highlight matched patterns in green

#### Sensitive Data Masking

Protect sensitive information in your MQTT traffic:

- **Custom Mask Patterns** (`-x, --mask`): Define which patterns to mask
- **Preserve Options** (`-p, --preserve`): Choose how much of the masked data to show:
  - `none`: Mask entire string
  - `first4`: Show first 4 characters
  - `last4`: Show last 4 characters
  - `both4`: Show first and last 4 characters
- **Toggle Masking**: Turn masking on/off with the 'm' key

#### Dynamic Table Sizing & Sorting

- Adjustable table width for optimal viewing
- Sort by timestamp or topic with visual indicators
- Real-time sorting toggle with keyboard shortcuts

The status bar at the bottom of the display shows:
- Mode information (live/retained, filtering, masking)
- Last update timestamp and message statistics
- Sorting mode and keyboard commands
- Auto-refresh status and countdown

## Development

```bash
# Run locally
bun start -t "#" -h localhost -d

# Clear retained messages
bun start -t "sensors/#" -h localhost --clear

# Run with advanced filtering and masking
bun start -t "#" -h localhost -f "NC-*" -x "password,token" -p "last4"
```

## License

MIT