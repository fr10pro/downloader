#!/usr/bin/env python3
import os
import re
import sqlite3
import asyncio
import logging
import datetime
import uuid
import requests
from io import BytesIO
from html import escape  # For HTML escaping in filenames
from flask import Flask
from pyrogram import Client, filters, enums, idle
from pyrogram.types import (
    InlineKeyboardButton, 
    InlineKeyboardMarkup,
    Message,
    CallbackQuery
)

# Configuration
API_ID = int(os.environ.get('API_ID', 28593211))
API_HASH = os.environ.get('API_HASH', '27ad7de4fe5cab9f8e310c5cc4b8d43d')
BOT_TOKEN = os.environ.get('BOT_TOKEN', '')
DATABASE_URL = os.environ.get('DATABASE_URL', 'bot.db')
ADMIN_USER_ID = int(os.environ.get('ADMIN_USER_ID', 5559075560))
PORT = int(os.environ.get('PORT', 5000))
FORWARD_CHANNEL = os.environ.get('FORWARD_CHANNEL')  # New environment variable for channel

# Initialize
app = Flask(__name__)
bot = Client("file-transfer-bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database setup with schema migration
def init_db():
    conn = sqlite3.connect(DATABASE_URL)
    c = conn.cursor()
    
    # Create users table
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    # Create stats table
    c.execute('''CREATE TABLE IF NOT EXISTS stats (
        downloads INTEGER DEFAULT 0,
        uploads INTEGER DEFAULT 0,
        users INTEGER DEFAULT 0
    )''')
    c.execute("INSERT OR IGNORE INTO stats (downloads, uploads, users) VALUES (0, 0, 0)")
    
    # Create thumbnails table with schema migration
    try:
        c.execute('''CREATE TABLE thumbnails (
            user_id INTEGER PRIMARY KEY,
            file_id TEXT NOT NULL,
            file_unique_id TEXT NOT NULL
        )''')
    except sqlite3.OperationalError:
        # Table already exists, check for missing columns
        c.execute("PRAGMA table_info(thumbnails)")
        columns = [col[1] for col in c.fetchall()]
        if 'file_id' not in columns:
            c.execute("ALTER TABLE thumbnails ADD COLUMN file_id TEXT NOT NULL DEFAULT ''")
        if 'file_unique_id' not in columns:
            c.execute("ALTER TABLE thumbnails ADD COLUMN file_unique_id TEXT NOT NULL DEFAULT ''")
    
    # Create pending_downloads table with schema migration
    try:
        c.execute('''CREATE TABLE pending_downloads (
            id TEXT PRIMARY KEY,
            user_id INTEGER,
            url TEXT,
            filename TEXT,
            file_size INTEGER,
            content_type TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
    except sqlite3.OperationalError:
        # Table already exists, check for missing columns
        c.execute("PRAGMA table_info(pending_downloads)")
        columns = [col[1] for col in c.fetchall()]
        if 'file_size' not in columns:
            c.execute("ALTER TABLE pending_downloads ADD COLUMN file_size INTEGER")
        if 'content_type' not in columns:
            c.execute("ALTER TABLE pending_downloads ADD COLUMN content_type TEXT")
    
    # Create channel table
    c.execute('''CREATE TABLE IF NOT EXISTS forward_channel (
        channel_id INTEGER PRIMARY KEY
    )''')
    
    conn.commit()
    conn.close()

# Initialize the database
init_db()

# Helper functions
def db_execute(query, args=(), fetchone=False):
    conn = sqlite3.connect(DATABASE_URL)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute(query, args)
        
        # Handle results before committing
        result = None
        if c.description:  # Check if there are results to fetch
            if fetchone:
                result = c.fetchone()
            else:
                result = c.fetchall()
        
        conn.commit()
        return result
    except sqlite3.Error as e:
        logger.error(f"Database error: {str(e)}")
        return None
    finally:
        conn.close()

def get_user(user_id):
    user = db_execute("SELECT * FROM users WHERE user_id = ?", (user_id,), fetchone=True)
    if not user:
        db_execute("INSERT INTO users (user_id) VALUES (?)", (user_id,))
        db_execute("UPDATE stats SET users = users + 1")
        user = db_execute("SELECT * FROM users WHERE user_id = ?", (user_id,), fetchone=True)
    return user

def save_file(file_id, user_id, original_name, file_size):
    db_execute(
        "UPDATE stats SET uploads = uploads + 1"
    )

def increment_downloads():
    db_execute("UPDATE stats SET downloads = downloads + 1")

def get_stats():
    return db_execute("SELECT * FROM stats", fetchone=True)

def format_size(size):
    if size is None or size == 0:
        return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"

# New thumbnail helper functions
def set_thumbnail(user_id, file_id, file_unique_id):
    db_execute(
        "INSERT OR REPLACE INTO thumbnails (user_id, file_id, file_unique_id) VALUES (?, ?, ?)",
        (user_id, file_id, file_unique_id)
    )

def get_thumbnail(user_id):
    return db_execute("SELECT * FROM thumbnails WHERE user_id = ?", (user_id,), fetchone=True)

def delete_thumbnail(user_id):
    db_execute("DELETE FROM thumbnails WHERE user_id = ?", (user_id,))

# New pending downloads helpers
def create_pending_download(user_id, url, filename, file_size, content_type):
    unique_id = str(uuid.uuid4())
    db_execute(
        "INSERT INTO pending_downloads (id, user_id, url, filename, file_size, content_type) VALUES (?, ?, ?, ?, ?, ?)",
        (unique_id, user_id, url, filename, file_size, content_type)
    )
    return unique_id

def get_pending_download(unique_id):
    return db_execute("SELECT * FROM pending_downloads WHERE id = ?", (unique_id,), fetchone=True)

def delete_pending_download(unique_id):
    db_execute("DELETE FROM pending_downloads WHERE id = ?", (unique_id,))

# Channel forwarding functions
def set_forward_channel(channel_id):
    db_execute("DELETE FROM forward_channel")
    db_execute("INSERT INTO forward_channel (channel_id) VALUES (?)", (channel_id,))

def get_forward_channel():
    row = db_execute("SELECT channel_id FROM forward_channel", fetchone=True)
    return row['channel_id'] if row else None

# Progress handler
class Progress:
    def __init__(self, message: Message, start_time):
        self.message = message
        self.start_time = start_time
        self.last_update = start_time
        self.current = 0
        self.total = 0
        self.speeds = []
        self.last_speed = 0
        
    async def progress_callback(self, current, total):
        self.current = current
        self.total = total
        
        now = datetime.datetime.now()
        if (now - self.last_update).seconds >= 1 or total == current:
            elapsed = (now - self.start_time).seconds or 1  # Prevent division by zero
            speed = (current - self.last_speed) / 1024  # KB/s
            self.speeds.append(speed)
            avg_speed = sum(self.speeds) / len(self.speeds) if self.speeds else 0
            
            # Format progress
            percentage = (current / total) * 100 if total > 0 else 100
            progress_bar = "[" + "■" * int(percentage/5) + "□" * (20 - int(percentage/5)) + "]"
            speed_str = f"{avg_speed:.2f} KB/s" if avg_speed < 1024 else f"{avg_speed/1024:.2f} MB/s"
            
            # Calculate ETA
            eta = (total - current) / (avg_speed * 1024) if avg_speed > 0 else 0
            eta_str = str(datetime.timedelta(seconds=int(eta))) if eta > 0 else "Calculating..."
            
            # Create message
            text = (
                f"**Transferring...**\n"
                f"{progress_bar} {percentage:.1f}%\n"
                f"**Size:** {format_size(total)}\n"
                f"**Speed:** {speed_str}\n"
                f"**ETA:** {eta_str}"
            )
            
            try:
                await self.message.edit_text(text)
            except Exception as e:
                logger.warning(f"Progress update failed: {str(e)}")
            
            self.last_update = now
            self.last_speed = current

# Bot handlers
@bot.on_message(filters.command("start"))
async def start_command(client: Client, message: Message):
    # Regular start command
    user = get_user(message.from_user.id)
    await message.reply_text(
        "📁 **File Transfer Bot**\n\n"
        "Send me any direct download link and I'll help you transfer it to Telegram!\n\n"
        "**Features:**\n"
        "• Direct download from any direct links\n"
        "• Real-time download progress\n"
        "• Fast Telegram uploads\n"
        "• Custom thumbnails\n"
        "• File format selection\n\n"
        "Use /help for full commands list",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("ℹ️ About", callback_data="about")  # New About button
            ]
        ])
    )

@bot.on_message(filters.command("help"))
async def help_command(client: Client, message: Message):
    await message.reply_text(
        "🛠 **Bot Commands:**\n\n"
        "/start - Welcome message\n"
        "/help - Show this help\n"
        "/stats - Show bot statistics\n"
        "/sethumbnail - Set a custom thumbnail (reply to an image)\n"
        "/viewthumbnail - View your current thumbnail\n"
        "/delthumbnail - Delete your thumbnail\n"
        "/addchannel - Set forwarding channel (admin only)\n"  # New command
        "/viewchannel - View current channel (admin only)\n"  # New command
        "\n"
        "**How to use:**\n"
        "1. Send any direct download link\n"
        "2. I'll download and show file info\n"
        "3. Choose upload format (if applicable)\n"
        "4. I'll upload to Telegram automatically\n"
    )

@bot.on_message(filters.command("stats"))
async def stats_command(client: Client, message: Message):
    stats = get_stats()
    if not stats:
        await message.reply_text("❌ Failed to retrieve statistics")
        return
        
    await message.reply_text(
        f"📊 **Bot Statistics:**\n\n"
        f"• Total Users: `{stats['users']}`\n"
        f"• Files Downloaded: `{stats['downloads']}`\n"
        f"• Files Uploaded: `{stats['uploads']}`"
    )

# ===== THUMBNAIL COMMANDS =====
@bot.on_message(filters.command("sethumbnail") & filters.private)
async def set_thumbnail_command(client: Client, message: Message):
    if not message.reply_to_message or not (message.reply_to_message.photo or message.reply_to_message.document):
        await message.reply_text("Please reply to an image message to set as thumbnail.")
        return

    if message.reply_to_message.photo:
        # Use largest photo size
        file_id = message.reply_to_message.photo.file_id
        file_unique_id = message.reply_to_message.photo.file_unique_id
    elif (message.reply_to_message.document and 
          message.reply_to_message.document.mime_type.startswith('image/')):
        file_id = message.reply_to_message.document.file_id
        file_unique_id = message.reply_to_message.document.file_unique_id
    else:
        await message.reply_text("The replied message is not a valid image.")
        return

    set_thumbnail(message.from_user.id, file_id, file_unique_id)
    await message.reply_text("✅ Thumbnail set successfully!")

@bot.on_message(filters.command("viewthumbnail") & filters.private)
async def view_thumbnail_command(client: Client, message: Message):
    thumbnail = get_thumbnail(message.from_user.id)
    if thumbnail:
        try:
            await client.send_photo(
                chat_id=message.chat.id,
                photo=thumbnail['file_id'],
                caption="Your current thumbnail"
            )
        except Exception as e:
            logger.error(f"Error sending thumbnail: {str(e)}")
            await message.reply_text("❌ Failed to send thumbnail. Please set a new one.")
    else:
        await message.reply_text("You haven't set a thumbnail yet.")

@bot.on_message(filters.command("delthumbnail") & filters.private)
async def del_thumbnail_command(client: Client, message: Message):
    delete_thumbnail(message.from_user.id)
    await message.reply_text("✅ Thumbnail deleted successfully.")

# ===== CHANNEL COMMANDS (ADMIN ONLY) =====
@bot.on_message(filters.command("addchannel") & filters.user(ADMIN_USER_ID))
async def add_channel_command(client: Client, message: Message):
    try:
        channel_id = int(message.command[1])
    except (IndexError, ValueError):
        await message.reply_text("Usage: /addchannel <channel_id>\nExample: /addchannel -1001234567890")
        return
        
    set_forward_channel(channel_id)
    await message.reply_text(f"✅ Files will now be forwarded to channel ID: `{channel_id}`")

@bot.on_message(filters.command("viewchannel") & filters.user(ADMIN_USER_ID))
async def view_channel_command(client: Client, message: Message):
    channel_id = get_forward_channel()
    if channel_id:
        await message.reply_text(f"📢 Current forwarding channel ID: `{channel_id}`")
    else:
        await message.reply_text("No forwarding channel set. Use /addchannel to set one.")

# ===== ABOUT CALLBACK HANDLER =====
@bot.on_callback_query(filters.regex(r"^about$"))
async def about_callback(client: Client, callback_query: CallbackQuery):
    await callback_query.answer()
    bot_username = (await client.get_me()).username
    await callback_query.message.edit_text(
        f"🤖 **File Transfer Bot**\n\n"
        f"**Developer:** [ORFIAI DEV](https://t.me/orfiai_dev)\n"
        f"**Bot Username:** @{bot_username}\n\n"
        "**Source Code:** [BUY](https://t.me/realemonfx)\n"
        "**Channel:** [ORFIAI](https://t.me/orfiai)\n\n"
        "This bot is actively maintained and updated regularly. "
        "Feel free to contribute to the project on GitHub!",
        disable_web_page_preview=True,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back", callback_data="back_to_start")]
        ])
    )

@bot.on_callback_query(filters.regex(r"^back_to_start$"))
async def back_to_start(client: Client, callback_query: CallbackQuery):
    await callback_query.answer()
    await start_command(client, callback_query.message)

# ===== LINK HANDLER WITH FORMAT SELECTION =====
@bot.on_message(filters.text & filters.private)
async def handle_links(client: Client, message: Message):
    # Skip commands
    if message.text.startswith('/'):
        return
    
    # Handle URL
    url = message.text.strip()
    if not re.match(r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', url):
        return
    
    msg = await message.reply_text("🔍 Analyzing URL...")
    
    try:
        # Get file info
        head = requests.head(url, allow_redirects=True, timeout=10)
        content_length = head.headers.get('content-length')
        content_type = head.headers.get('content-type', '')
        filename = os.path.basename(url)
        
        if not content_length:
            raise Exception("Could not determine file size")
            
        file_size = int(content_length)
        
        # Save as pending download and ask for format
        pending_id = create_pending_download(
            message.from_user.id,
            url,
            filename,
            file_size,
            content_type
        )
        
        # Create format selection buttons
        buttons = []
        if 'video' in content_type:
            buttons.append(InlineKeyboardButton("Video", callback_data=f"format:{pending_id}:video"))
        buttons.append(InlineKeyboardButton("Document", callback_data=f"format:{pending_id}:document"))
        
        await msg.edit_text(
            f"📥 **File Information:**\n\n"
            f"• **File Name:** `{filename}`\n"
            f"• **File Size:** `{format_size(file_size)}`\n\n"
            f"Please choose upload format:",
            reply_markup=InlineKeyboardMarkup([buttons])
        )
        
    except Exception as e:
        logger.error(f"Download error: {str(e)}", exc_info=True)
        await msg.edit_text(f"❌ Error: {str(e)}")

# ===== CALLBACK HANDLER FOR FORMAT SELECTION =====
@bot.on_callback_query(filters.regex(r"^format:"))
async def format_choice_callback(client: Client, callback_query: CallbackQuery):
    data = callback_query.data.split(':')
    if len(data) != 3:
        await callback_query.answer("Invalid request", show_alert=True)
        return
        
    pending_id = data[1]
    format_choice = data[2]
    
    pending = get_pending_download(pending_id)
    if not pending:
        await callback_query.answer("Download session expired", show_alert=True)
        await callback_query.message.delete()
        return
        
    # Delete pending record to prevent reuse
    delete_pending_download(pending_id)
    
    await callback_query.answer(f"Starting {format_choice} upload...")
    msg = await callback_query.message.edit_text("Starting download...")
    
    try:
        url = pending['url']
        filename = pending['filename']
        file_size = pending['file_size']
        content_type = pending['content_type']
        
        # Start download
        start_time = datetime.datetime.now()
        progress = Progress(msg, start_time)
        
        # Download file
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()
        
        # Create temporary file
        temp_file = f"downloads/{filename}"
        os.makedirs("downloads", exist_ok=True)
        
        with open(temp_file, 'wb') as f:
            downloaded = 0
            last_update = start_time
            
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    now = datetime.datetime.now()
                    if (now - last_update).seconds >= 1 or downloaded == file_size:
                        await progress.progress_callback(downloaded, file_size)
                        last_update = now
        
        increment_downloads()
        
        # Get user's thumbnail if exists
        thumbnail = get_thumbnail(callback_query.from_user.id)
        thumbnail_file_id = thumbnail['file_id'] if thumbnail else None
        
        # Upload with selected format
        await upload_file(
            client, 
            msg, 
            temp_file, 
            filename, 
            content_type, 
            file_size,
            url,  # Pass original URL
            as_video=(format_choice == "video"),
            thumbnail=thumbnail_file_id
        )
        
    except Exception as e:
        logger.error(f"Download error: {str(e)}", exc_info=True)
        await msg.edit_text(f"❌ Error: {str(e)}")

# ===== UPLOAD FUNCTION WITH STYLED FILENAME CAPTION AND CHANNEL FORWARDING =====
async def upload_file(
    client: Client, 
    message: Message, 
    filepath, 
    filename, 
    content_type, 
    file_size,
    original_url,  # Added original URL parameter
    as_video=False,
    thumbnail=None
):
    msg = await message.edit_text("📤 Uploading file to Telegram...")
    start_time = datetime.datetime.now()
    progress = Progress(msg, start_time)
    
    try:
        # Handle thumbnail properly
        thumbnail_bytes = None
        if thumbnail:
            try:
                # Download thumbnail to memory
                thumbnail_data = await client.download_media(thumbnail, in_memory=True)
                if thumbnail_data:
                    # Move to the end to get the size
                    thumbnail_data.seek(0, 2)
                    size = thumbnail_data.tell()
                    # Move back to the beginning
                    thumbnail_data.seek(0)
                    if size == 0:
                        logger.warning("Downloaded thumbnail has 0 bytes, skipping")
                    else:
                        thumbnail_data.name = "thumbnail.jpg"
                        thumbnail_bytes = thumbnail_data
            except Exception as e:
                logger.error(f"Failed to download thumbnail: {str(e)}")

        # Get bot username for caption
        bot_username = (await client.get_me()).username

        # Prepare file caption with styled filename
        styled_filename = f"<code>{escape(filename)}</code>"  # Stylish monospace formatting
        file_caption = f"@{bot_username} {styled_filename}"

        # Determine file type with format choice
        if as_video and 'video' in content_type:
            sent_msg = await client.send_video(
                chat_id=message.chat.id,
                video=filepath,
                file_name=filename,
                caption=file_caption,
                parse_mode=enums.ParseMode.HTML,
                progress=progress.progress_callback,
                supports_streaming=True,
                thumb=thumbnail_bytes or None
            )
            file_id = sent_msg.video.file_id
        else:
            sent_msg = await client.send_document(
                chat_id=message.chat.id,
                document=filepath,
                file_name=filename,
                caption=file_caption,
                parse_mode=enums.ParseMode.HTML,
                progress=progress.progress_callback,
                thumb=thumbnail_bytes or None
            )
            file_id = sent_msg.document.file_id
        
        # Save file reference
        save_file(file_id, message.from_user.id, filename, file_size)
        
        # Forward to channel if configured
        channel_id = get_forward_channel()
        if channel_id:
            try:
                await sent_msg.copy(
                    chat_id=channel_id,
                    caption=f"📥 Uploaded by user\n\n" + file_caption
                )
                logger.info(f"File forwarded to channel: {channel_id}")
            except Exception as e:
                logger.error(f"Failed to forward to channel: {str(e)}")
                await client.send_message(
                    ADMIN_USER_ID,
                    f"❌ Failed to forward file to channel {channel_id}:\n{str(e)}"
                )
        
        # Final progress update
        await progress.progress_callback(file_size, file_size)
        await asyncio.sleep(1)  # Let user see 100% progress
        
        await msg.edit_text(
            f"✅ **File uploaded successfully!**\n\n"
            f"• **File Name:** `{filename}`\n"
            f"• **File Size:** `{format_size(file_size)}`\n"
            f"• **Format:** {'Video' if as_video else 'Document'}\n\n"
            f"🔗 Direct Link: `{original_url}`"
        )
        
        # Clean up
        try:
            os.remove(filepath)
        except Exception as e:
            logger.error(f"Error deleting file: {str(e)}")
        
    except Exception as e:
        logger.error(f"Upload error: {str(e)}", exc_info=True)
        await msg.edit_text(f"❌ Upload failed: {str(e)}")
        try:
            os.remove(filepath)
        except:
            pass

# Run the bot
async def run_bot():
    await bot.start()
    logging.info("Bot started")
    
    # Notify admin about channel status
    channel_id = get_forward_channel()
    channel_status = f"Channel ID: {channel_id}" if channel_id else "No channel set"
    await bot.send_message(ADMIN_USER_ID, f"✅ Bot started successfully!\n{channel_status}")
    
    await idle()

if __name__ == "__main__":
    # Create directories if not exist
    os.makedirs("downloads", exist_ok=True)
    
    # Start Flask server in a separate thread
    from threading import Thread
    flask_thread = Thread(target=lambda: app.run(host='0.0.0.0', port=PORT))
    flask_thread.daemon = True
    flask_thread.start()
    
    # Run the bot with graceful shutdown
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run_bot())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
    finally:
        loop.run_until_complete(bot.stop())
        logging.info("Bot stopped")
