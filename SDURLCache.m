//
//  SDURLCache.m
//  SDURLCache
//
//  Created by Olivier Poitrey on 15/03/10.
//  Copyright 2010 Dailymotion. All rights reserved.
//

#import "SDURLCache.h"
#import <CommonCrypto/CommonDigest.h>

static NSTimeInterval const kSDURLCacheInfoDefaultMinCacheInterval = 5 * 60; // 5 minute
static NSString *const kSDURLCacheInfoFileName = @"cacheInfo.db";
static NSString *const kSDURLCacheInfoDiskUsageKey = @"diskUsage";
static NSString *const kSDURLCacheInfoAccessesKey = @"accesses";
static NSString *const kSDURLCacheInfoSizesKey = @"sizes";
static float const kSDURLCacheLastModFraction = 0.1f; // 10% since Last-Modified suggested by RFC2616 section 13.2.4
static float const kSDURLCacheDefault = 3600; // Default cache expiration delay if none defined (1 hour)

static NSDateFormatter* CreateDateFormatter(NSString *format)
{
    NSDateFormatter *dateFormatter = [[NSDateFormatter alloc] init];
    [dateFormatter setLocale:[[[NSLocale alloc] initWithLocaleIdentifier:@"en_US"] autorelease]];
    [dateFormatter setTimeZone:[NSTimeZone timeZoneWithAbbreviation:@"GMT"]];
    [dateFormatter setDateFormat:format];
    return [dateFormatter autorelease];
}

@implementation NSCachedURLResponse(NSCoder)

- (void)encodeWithCoder:(NSCoder *)coder
{
    [coder encodeDataObject:self.data];
    [coder encodeObject:self.response forKey:@"response"];
    [coder encodeObject:self.userInfo forKey:@"userInfo"];
    [coder encodeInt:self.storagePolicy forKey:@"storagePolicy"];
}

- (id)initWithCoder:(NSCoder *)coder
{
    return [self initWithResponse:[coder decodeObjectForKey:@"response"]
                             data:[coder decodeDataObject]
                         userInfo:[coder decodeObjectForKey:@"userInfo"]
                    storagePolicy:[coder decodeIntForKey:@"storagePolicy"]];
}

@end


@interface SDURLCache ()
@property (nonatomic, retain) NSString *diskCachePath;
@property (nonatomic, retain) NSOperationQueue *ioQueue;
@property (retain) NSOperation *periodicMaintenanceOperation;
- (void)periodicMaintenance;
@end

@implementation SDURLCache

@synthesize diskCachePath, minCacheInterval, ioQueue, periodicMaintenanceOperation, ignoreMemoryOnlyStoragePolicy;

#pragma mark SDURLCache (tools)

+ (NSURLRequest *)canonicalRequestForRequest:(NSURLRequest *)request
{
    NSString *string = request.URL.absoluteString;
    NSRange hash = [string rangeOfString:@"#"];
    if (hash.location == NSNotFound)
        return request;

    NSMutableURLRequest *copy = [[request mutableCopy] autorelease];
    copy.URL = [NSURL URLWithString:[string substringToIndex:hash.location]];
    return copy;
}

+ (NSString *)fileNameForURL:(NSURL *)url
{
    const char *str = [url.absoluteString UTF8String];
    unsigned char r[CC_MD5_DIGEST_LENGTH];
    CC_MD5(str, strlen(str), r);
    return [NSString stringWithFormat:@"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
            r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10], r[11], r[12], r[13], r[14], r[15]];
}

/*
 * Parse HTTP Date: http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.3.1
 */
+ (NSDate *)dateFromHttpDateString:(NSString *)httpDate
{
    static NSDateFormatter *RFC1123DateFormatter;
    static NSDateFormatter *ANSICDateFormatter;
    static NSDateFormatter *RFC850DateFormatter;
    NSDate *date = nil;

    @synchronized(self) // NSDateFormatter isn't thread safe
    {
        // RFC 1123 date format - Sun, 06 Nov 1994 08:49:37 GMT
        if (!RFC1123DateFormatter) RFC1123DateFormatter = [CreateDateFormatter(@"EEE, dd MMM yyyy HH:mm:ss z") retain];
        date = [RFC1123DateFormatter dateFromString:httpDate];
        if (!date)
        {
            // ANSI C date format - Sun Nov  6 08:49:37 1994
            if (!ANSICDateFormatter) ANSICDateFormatter = [CreateDateFormatter(@"EEE MMM d HH:mm:ss yyyy") retain];
            date = [ANSICDateFormatter dateFromString:httpDate];
            if (!date)
            {
                // RFC 850 date format - Sunday, 06-Nov-94 08:49:37 GMT
                if (!RFC850DateFormatter) RFC850DateFormatter = [CreateDateFormatter(@"EEEE, dd-MMM-yy HH:mm:ss z") retain];
                date = [RFC850DateFormatter dateFromString:httpDate];
            }
        }
    }

    return date;
}

/*
 * This method tries to determine the expiration date based on a response headers dictionary.
 */
+ (NSDate *)expirationDateFromHeaders:(NSDictionary *)headers withStatusCode:(NSInteger)status
{
    if (status != 200 && status != 203 && status != 300 && status != 301 && status != 302 && status != 307 && status != 410)
    {
        // Uncacheable response status code
        return nil;
    }

    // Check Pragma: no-cache
    NSString *pragma = [headers objectForKey:@"Pragma"];
    if (pragma && [pragma isEqualToString:@"no-cache"])
    {
        // Uncacheable response
        return nil;
    }

    // Define "now" based on the request
    NSString *date = [headers objectForKey:@"Date"];
    NSDate *now;
    if (date)
    {
        now = [SDURLCache dateFromHttpDateString:date];
    }
    else
    {
        // If no Date: header, define now from local clock
        now = [NSDate date];
    }

    // Look at info from the Cache-Control: max-age=n header
    NSString *cacheControl = [headers objectForKey:@"Cache-Control"];
    if (cacheControl)
    {
        NSRange foundRange = [cacheControl rangeOfString:@"no-store"];
        if (foundRange.length > 0)
        {
            // Can't be cached
            return nil;
        }

        NSInteger maxAge;
        foundRange = [cacheControl rangeOfString:@"max-age="];
        if (foundRange.length > 0)
        {
            NSScanner *cacheControlScanner = [NSScanner scannerWithString:cacheControl];
            [cacheControlScanner setScanLocation:foundRange.location + foundRange.length];
            if ([cacheControlScanner scanInteger:&maxAge])
            {
                if (maxAge > 0)
                {
                    return [NSDate dateWithTimeIntervalSinceNow:maxAge];
                }
                else
                {
                    return nil;
                }
            }
        }
    }

    // If not Cache-Control found, look at the Expires header
    NSString *expires = [headers objectForKey:@"Expires"];
    if (expires)
    {
        NSTimeInterval expirationInterval = 0;
        NSDate *expirationDate = [SDURLCache dateFromHttpDateString:expires];
        if (expirationDate)
        {
            expirationInterval = [expirationDate timeIntervalSinceDate:now];
        }
        if (expirationInterval > 0)
        {
            // Convert remote expiration date to local expiration date
            return [NSDate dateWithTimeIntervalSinceNow:expirationInterval];
        }
        else
        {
            // If the Expires header can't be parsed or is expired, do not cache
            return nil;
        }
    }

    if (status == 302 || status == 307)
    {
        // If not explict cache control defined, do not cache those status
        return nil;
    }

    // If no cache control defined, try some heristic to determine an expiration date
    NSString *lastModified = [headers objectForKey:@"Last-Modified"];
    if (lastModified)
    {
        NSTimeInterval age = 0;
        NSDate *lastModifiedDate = [SDURLCache dateFromHttpDateString:lastModified];
        if (lastModifiedDate)
        {
            // Define the age of the document by comparing the Date header with the Last-Modified header
            age = [now timeIntervalSinceDate:lastModifiedDate];
        }
        if (age > 0)
        {
            return [NSDate dateWithTimeIntervalSinceNow:(age * kSDURLCacheLastModFraction)];
        }
        else
        {
            return nil;
        }
    }

    // If nothing permitted to define the cache expiration delay nor to restrict its cacheability, use a default cache expiration delay
    return [[[NSDate alloc] initWithTimeInterval:kSDURLCacheDefault sinceDate:now] autorelease];

}

#pragma mark SDURLCache (private)

- (void)removeCachedResponseForURLs:(NSArray *)urls
{
    NSAutoreleasePool *pool = [[NSAutoreleasePool alloc] init];
    NSFileManager *fileManager = [[NSFileManager alloc] init];
    
    OSSpinLockLock(&spinLock);
    sqlite3_exec(database, "BEGIN;", NULL, NULL, NULL);
    for (NSURL *url in urls) {
        [fileManager removeItemAtPath:[diskCachePath stringByAppendingPathComponent:[SDURLCache fileNameForURL:url]] error:NULL];
        sqlite3_stmt *stmt = NULL;
        const char *urlString = [[url absoluteString] UTF8String];
        size_t length = strlen(urlString);
        sqlite_int64 size = 0;
        if (sqlite3_prepare_v2(database, "SELECT size FROM cacheEntries WHERE url = ?;", -1, &stmt, NULL)) {
            sqlite3_bind_text(stmt, 1, urlString, length, SQLITE_TRANSIENT);
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                size += sqlite3_column_int64(stmt, 0);
            }
            sqlite3_finalize(stmt);
        }
        if (sqlite3_prepare_v2(database, "UPDATE diskUsage SET totalSize = totalSize - ?;DELETE FROM cacheEntries WHERE url = ?;", -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_int64(stmt, 1, size);
            sqlite3_bind_text(stmt, 2, urlString, length, SQLITE_TRANSIENT);
            while (sqlite3_step(stmt) == SQLITE_ROW) {
            }
            sqlite3_finalize(stmt);
            diskCacheUsage -= size;
        }
    }
    sqlite3_exec(database, "COMMIT;", NULL, NULL, NULL);
    OSSpinLockUnlock(&spinLock);

    [fileManager release];
    [pool drain];
}

- (void)balanceDiskUsage
{
    if (diskCacheUsage < self.diskCapacity)
    {
        // Already done
        return;
    }

    NSMutableArray *urlsToRemove = [NSMutableArray array];

    OSSpinLockLock(&spinLock);
    sqlite3_stmt *stmt = NULL;
    sqlite3_int64 capacityToSave = diskCacheUsage - self.diskCapacity;
    if (sqlite3_prepare_v2(database, "SELECT url, size FROM cacheEntries ORDER BY accessData;", -1, &stmt, NULL) != SQLITE_OK) {
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            const void *urlString = sqlite3_column_text(stmt, 0);
            if (urlString) {
                [urlsToRemove addObject:[NSURL URLWithString:[NSString stringWithUTF8String:urlString]]];
                capacityToSave -= sqlite3_column_int64(stmt, 1);
            }
            if (capacityToSave <= 0)
                break;
        }
        sqlite3_finalize(stmt);
    }
    OSSpinLockUnlock(&spinLock);

    [self removeCachedResponseForURLs:urlsToRemove];
}


- (void)storeToDisk:(NSDictionary *)context
{
    NSURLRequest *request = [context objectForKey:@"request"];
    NSCachedURLResponse *cachedResponse = [context objectForKey:@"cachedResponse"];

    NSURL *url = request.URL;
    NSString *cacheKey = [SDURLCache fileNameForURL:url];
    NSString *cacheFilePath = [diskCachePath stringByAppendingPathComponent:cacheKey];


    // Archive the cached response on disk
    if (![NSKeyedArchiver archiveRootObject:cachedResponse toFile:cacheFilePath])
    {
        // Caching failed for some reason
        return;
    }

    // Update disk usage info
    NSFileManager *fileManager = [[NSFileManager alloc] init];
    long long cacheItemSize = [[[fileManager attributesOfItemAtPath:cacheFilePath error:NULL] objectForKey:NSFileSize] longLongValue];
    [fileManager release];

    OSSpinLockLock(&spinLock);

    sqlite3_exec(database, "BEGIN;", NULL, NULL, NULL);
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(database, "INSERT INTO cacheEntries (url, accessDate, size) VALUES (?, ?, ?);", -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, [[url absoluteString] UTF8String], -1, SQLITE_TRANSIENT);
        sqlite3_bind_double(stmt, 2, [NSDate timeIntervalSinceReferenceDate]);
        sqlite3_bind_int64(stmt, 3, cacheItemSize);
        while (sqlite3_step(stmt) == SQLITE_ROW) {
        }
        sqlite3_finalize(stmt);
    }
    if (sqlite3_prepare_v2(database, "UPDATE diskUsage SET totalSize = totalSize + ?;", -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(stmt, 1, cacheItemSize);
        while (sqlite3_step(stmt) == SQLITE_ROW) {
        }
        sqlite3_finalize(stmt);
    }
    sqlite3_exec(database, "COMMIT;", NULL, NULL, NULL);
    
    diskCacheUsage += cacheItemSize;

    OSSpinLockUnlock(&spinLock);
}

- (void)periodicMaintenance
{
    // If another same maintenance operation is already sceduled, cancel it so this new operation will be executed after other
    // operations of the queue, so we can group more work together
    [periodicMaintenanceOperation cancel];
    self.periodicMaintenanceOperation = nil;

    // If disk usage outrich capacity, run the cache eviction operation and if cacheInfo dictionnary is dirty, save it in an operation
    if (diskCacheUsage > self.diskCapacity)
    {
        self.periodicMaintenanceOperation = [[[NSInvocationOperation alloc] initWithTarget:self selector:@selector(balanceDiskUsage) object:nil] autorelease];
        [ioQueue addOperation:periodicMaintenanceOperation];
    }
}

#pragma mark SDURLCache

+ (NSString *)defaultCachePath
{
    NSArray *paths = NSSearchPathForDirectoriesInDomains(NSCachesDirectory, NSUserDomainMask, YES);
    return [[paths objectAtIndex:0] stringByAppendingPathComponent:@"SDURLCacheLite"];
}

#pragma mark NSURLCache

- (id)initWithMemoryCapacity:(NSUInteger)memoryCapacity diskCapacity:(NSUInteger)diskCapacity diskPath:(NSString *)path
{
    if ((self = [super initWithMemoryCapacity:memoryCapacity diskCapacity:diskCapacity diskPath:path]))
    {
        self.minCacheInterval = kSDURLCacheInfoDefaultMinCacheInterval;
        self.diskCachePath = path;

        NSFileManager *fileManager = [[NSFileManager alloc] init];
        if (![fileManager fileExistsAtPath:diskCachePath])
        {
            [fileManager createDirectoryAtPath:diskCachePath
                   withIntermediateDirectories:YES
                                    attributes:nil
                                         error:NULL];
        }
        [fileManager release];
        
        if (sqlite3_open_v2([[path stringByAppendingPathComponent:kSDURLCacheInfoFileName] UTF8String], &database, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, NULL) == SQLITE_OK) {
            if (sqlite3_exec(database, "PRAGMA synchronous = 1;PRAGMA auto_vacuum = 0;VACUUM;BEGIN;CREATE TABLE IF NOT EXISTS diskUsage (totalSize INT);CREATE TABLE IF NOT EXISTS cacheEntries (url TEXT, accessDate REAL, size INT);CREATE UNIQUE INDEX IF NOT EXISTS cacheEntries_url_index ON cacheEntries (url);", NULL, NULL, NULL) == SQLITE_OK) {
                sqlite3_stmt *stmt = NULL;
                if (sqlite3_prepare_v2(database, "SELECT totalSize FROM diskUsage;", -1, &stmt, NULL) == SQLITE_OK) {
                    BOOL exists = NO;
                    while (sqlite3_step(stmt) == SQLITE_ROW) {
                        diskCacheUsage = sqlite3_column_int64(stmt, 0);
                        exists = YES;
                    }
                    sqlite3_finalize(stmt);
                    if (!exists) {
                        sqlite3_exec(database, "INSERT INTO diskUsage (totalSize) VALUES (0);", NULL, NULL, NULL);
                    }
                }
                int error = sqlite3_exec(database, "COMMIT;", NULL, NULL, NULL);
                if (error != SQLITE_OK) {
                    NSLog(@"Failed with error %d", error);
                }
            }
        }
        
        // Init the operation queue
        self.ioQueue = [[[NSOperationQueue alloc] init] autorelease];
        ioQueue.maxConcurrentOperationCount = 1; // used to streamline operations in a separate thread

        self.ignoreMemoryOnlyStoragePolicy = YES;
        
        periodicMaintenanceTimer = [[NSTimer scheduledTimerWithTimeInterval:5
                                                                     target:self
                                                                   selector:@selector(periodicMaintenance)
                                                                   userInfo:nil
                                                                    repeats:YES] retain];
	}

    return self;
}

- (void)storeCachedResponse:(NSCachedURLResponse *)cachedResponse forRequest:(NSURLRequest *)request
{
    request = [SDURLCache canonicalRequestForRequest:request];

    if (request.cachePolicy == NSURLRequestReloadIgnoringLocalCacheData
        || request.cachePolicy == NSURLRequestReloadIgnoringLocalAndRemoteCacheData
        || request.cachePolicy == NSURLRequestReloadIgnoringCacheData)
    {
        // When cache is ignored for read, it's a good idea not to store the result as well as this option
        // have big chance to be used every times in the future for the same request.
        // NOTE: This is a change regarding default URLCache behavior
        return;
    }

    [super storeCachedResponse:cachedResponse forRequest:request];

    NSURLCacheStoragePolicy storagePolicy = cachedResponse.storagePolicy;
    if ((storagePolicy == NSURLCacheStorageAllowed || (storagePolicy == NSURLCacheStorageAllowedInMemoryOnly && ignoreMemoryOnlyStoragePolicy))
        && [cachedResponse.response isKindOfClass:[NSHTTPURLResponse self]]
        && cachedResponse.data.length < self.diskCapacity)
    {
        NSDictionary *headers = [(NSHTTPURLResponse *)cachedResponse.response allHeaderFields];
        // RFC 2616 section 13.3.4 says clients MUST use Etag in any cache-conditional request if provided by server
        if (![headers objectForKey:@"Etag"])
        {
            NSDate *expirationDate = [SDURLCache expirationDateFromHeaders:headers
                                                            withStatusCode:((NSHTTPURLResponse *)cachedResponse.response).statusCode];
            if (!expirationDate || [expirationDate timeIntervalSinceNow] - minCacheInterval <= 0)
            {
                // This response is not cacheable, headers said
                return;
            }
        }

        [ioQueue addOperation:[[[NSInvocationOperation alloc] initWithTarget:self
                                                                    selector:@selector(storeToDisk:)
                                                                      object:[NSDictionary dictionaryWithObjectsAndKeys:
                                                                              cachedResponse, @"cachedResponse",
                                                                              request, @"request",
                                                                              nil]] autorelease]];
    }
}

- (NSCachedURLResponse *)cachedResponseForRequest:(NSURLRequest *)request
{
    request = [SDURLCache canonicalRequestForRequest:request];

    NSCachedURLResponse *memoryResponse = [super cachedResponseForRequest:request];
    if (memoryResponse)
    {
        return memoryResponse;
    }
    
    NSURL *url = request.URL;

    NSCachedURLResponse *diskResponse = nil;
    @try {
        diskResponse = [NSKeyedUnarchiver unarchiveObjectWithFile:[diskCachePath stringByAppendingPathComponent:[SDURLCache fileNameForURL:url]]];
    }
    @catch(id e) {
    }
    if (!diskResponse)
        return nil;

    OSSpinLockLock(&spinLock);

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(database, "UPDATE cacheEntries SET accessDate = ? WHERE url = ?;", -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_double(stmt, 1, [NSDate timeIntervalSinceReferenceDate]);
        sqlite3_bind_text(stmt, 2, [[url absoluteString] UTF8String], -1, NULL);
        while (sqlite3_step(stmt) == SQLITE_ROW) {
        }
        sqlite3_finalize(stmt);
    }

    OSSpinLockUnlock(&spinLock);
    
    // OPTI: Store the response to memory cache for potential future requests
    [super storeCachedResponse:diskResponse forRequest:request];
    
    // SRK: Work around an interesting retainCount bug in CFNetwork on iOS << 3.2.
    if (kCFCoreFoundationVersionNumber < kCFCoreFoundationVersionNumber_iPhoneOS_3_2)
    {
        diskResponse = [super cachedResponseForRequest:request];
    }
    
    return diskResponse;
}

- (NSUInteger)currentDiskUsage
{
    return diskCacheUsage;
}

- (void)removeCachedResponseForRequest:(NSURLRequest *)request
{
    request = [SDURLCache canonicalRequestForRequest:request];

    [super removeCachedResponseForRequest:request];
    [self removeCachedResponseForURLs:[NSArray arrayWithObject:request.URL]];
}

- (void)removeAllCachedResponses
{
    [super removeAllCachedResponses];
    NSFileManager *fileManager = [[NSFileManager alloc] init];
    OSSpinLockLock(&spinLock);

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(database, "SELECT url FROM cacheEntries;", -1, &stmt, NULL) == SQLITE_OK) {
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            NSURL *url = [NSURL URLWithString:[NSString stringWithUTF8String:(const char *)sqlite3_column_text(stmt, 0)]];
            NSString *filePath = [diskCachePath stringByAppendingPathComponent:[SDURLCache fileNameForURL:url]];
            [fileManager removeItemAtPath:filePath error:NULL];
        }
        sqlite3_finalize(stmt);
        sqlite3_exec(database, "BEGIN;UPDATE diskUsage SET totalSize = 0;DELETE FROM cacheEntries;COMMIT;", NULL, NULL, NULL);
        diskCacheUsage = 0;
    }

    OSSpinLockUnlock(&spinLock);
    [fileManager release];
}

- (BOOL)isCached:(NSURL *)url
{
    NSURLRequest *request = [NSURLRequest requestWithURL:url];
    request = [SDURLCache canonicalRequestForRequest:request];

    if ([super cachedResponseForRequest:request])
    {
        return YES;
    }
    NSString *cacheKey = [SDURLCache fileNameForURL:url];
    NSString *cacheFile = [diskCachePath stringByAppendingPathComponent:cacheKey];
    if ([[[[NSFileManager alloc] init] autorelease] fileExistsAtPath:cacheFile])
    {
        return YES;
    }
    return NO;
}

#pragma mark NSObject

- (void)dealloc
{
    [periodicMaintenanceTimer invalidate];
    [periodicMaintenanceTimer release], periodicMaintenanceTimer = nil;
    [periodicMaintenanceOperation release], periodicMaintenanceOperation = nil;
    sqlite3_close(database);
    [ioQueue release], ioQueue = nil;
    [super dealloc];
}


@end
