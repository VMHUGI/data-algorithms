import discord
from discord.ext import commands

bot = commands.Bot(command_prefix='%')

@bot.command()
async def ping(ctx):
    await ctx.send('pong')

@bot.command()
async def status(ctx):
    await ctx.send('Se envía el status de las tablas UGI')


def startugiops_bot():
    print("UGIOPS - BOT STARTED")
    bot.run('ODA5OTU0MjA5NTc4NDgzNzIy.YCcmow.3P8baHTCCeBfJiL930qa2T-xBJ0')
    print("UGIOPS - BOT finished - Good Bye")
