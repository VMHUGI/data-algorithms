import ftplib

def conectorFTP(ftp_server,ftp_user,ftp_password):
    ftp = ftplib.FTP(ftp_server)
    ftp.login(ftp_user, ftp_password)
    return ftp