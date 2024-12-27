import time
import string
import random
import datetime
import aiofiles
import asyncio
import traceback
import aiofiles.os
from configs import Config
from pyrogram.errors import (
    FloodWait,
    InputUserDeactivated,
    UserIsBlocked,
    PeerIdInvalid
)

broadcast_ids = {}


async def send_msg(user_id, message):
    try:
        if Config.BROADCAST_AS_COPY is False:
            await message.forward(chat_id=user_id)
        elif Config.BROADCAST_AS_COPY is True:
            await message.copy(chat_id=user_id)
        return 200, None
    except FloodWait as e:
        await asyncio.sleep(e.value)
        return send_msg(user_id, message)
    except InputUserDeactivated:
        return 400, f"{user_id} : deactivated\n"
    except UserIsBlocked:
        return 400, f"{user_id} : blocked the bot\n"
    except PeerIdInvalid:
        return 400, f"{user_id} : user id invalid\n"
    except Exception as e:
        return 500, f"{user_id} : {traceback.format_exc()}\n"


async def main_broadcast_handler(m, db):
    all_users = await db.get_all_users()
    broadcast_msg = m.reply_to_message
    while True:
        broadcast_id = ''.join([random.choice(string.ascii_letters) for i in range(3)])
        if not broadcast_ids.get(broadcast_id):
            break
    out = await m.reply_text(
        text=f"Broadcast Started! You will be notified with log file when all the users are notified."
    )
    start_time = time.time()
    total_users = await db.total_users_count()
    done = 0
    failed = 0
    success = 0
    broadcast_ids[broadcast_id] = dict(
        total=total_users,
        current=done,
        failed=failed,
        success=success
    )
    async with aiofiles.open('broadcast.txt', 'w') as broadcast_log_file:
        async for user in all_users:
            try:
                # Send the broadcast message and store the sent message's ID
                sent_message = await m.copy(chat_id=user['id'], message=broadcast_msg)
                message_ids_to_delete.append((user['id'], sent_message.id))  # Store user_id and message_id

                sts, msg = await send_msg(
                    user_id=int(user['id']),
                    message=broadcast_msg
                )
                if msg is not None:
                    await broadcast_log_file.write(msg)
                if sts == 200:
                    success += 1
                else:
                    failed += 1
                if sts == 400:
                    await db.delete_user(user['id'])
                done += 1
                if broadcast_ids.get(broadcast_id) is None:
                    break
                else:
                    broadcast_ids[broadcast_id].update(
                        dict(
                            current=done,
                            failed=failed,
                            success=success
                        )
                    )
            except Exception as e:
                print(f"Error sending message to user {user['id']}: {e}")
    
    # If broadcast completed, remove broadcast_id from the dictionary
    if broadcast_ids.get(broadcast_id):
        broadcast_ids.pop(broadcast_id)

    completed_in = datetime.timedelta(seconds=int(time.time() - start_time))
    await out.delete()  # Delete the broadcast start message

    # Provide final report message or document
    if failed == 0:
        await m.reply_text(
            text=f"Broadcast completed in `{completed_in}`\n\nTotal users {total_users}.\nTotal done {done}, {success} success and {failed} failed.",
            quote=True
        )
    else:
        await m.reply_document(
            document='broadcast.txt',
            caption=f"Broadcast completed in `{completed_in}`\n\nTotal users {total_users}.\nTotal done {done}, {success} success and {failed} failed.",
            quote=True
        )

    # Wait for 1 minute (60 seconds) before deleting all broadcast messages
    await asyncio.sleep(60)  # Wait 60 seconds (1 minute) before deleting the broadcast messages

    # Delete the broadcast messages for all users
    for user_id, message_id in message_ids_to_delete:
        try:
            await m.client.delete_messages(chat_id=user_id, message_ids=message_id)  # Delete the message for each user
            print(f"Deleted message {message_id} for user {user_id}")
        except Exception as e:
            print(f"Failed to delete message {message_id} for user {user_id}: {e}")

    # Delete the log file after sending the final report
    await aiofiles.os.remove('broadcast.txt')
