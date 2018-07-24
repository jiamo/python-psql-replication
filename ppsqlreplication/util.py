

def int_lsn_to_str(lsn):
    return "%X/%08X" % ((lsn >> 32) & 0xFFFFFFFF, lsn & 0xFFFFFFFF)


def str_lsn_to_int(lsn):
    lsn = lsn.split('/')
    high = int(lsn[0], 16)
    low = int(lsn[1], 16)
    return ((high & 0xFFFFFFFF) << 32) + (low & 0xFFFFFFFF)
