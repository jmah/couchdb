//
//  CouchbaseCallbacks.m
//  iErl14
//
//  Created by Jens Alfke on 10/3/11.
//  Copyright (c) 2011 Couchbase, Inc. All rights reserved.
//

#import "CouchbaseCallbacks.h"


typedef enum {
    kMapBlock,
    kReduceBlock,
    kValidateUpdateBlock,
    // add new ones here
    kNumBlockTypes
} BlockType;


typedef uint32_t Uint32;

/* ==== From global.h ==== */
typedef struct {
    Uint32 state[4];		/* state (ABCD) */
    Uint32 count[2];		/* number of bits, modulo 2^64 (lsb first) */
    unsigned char buffer[64];	/* input buffer */
} MD5_CTX;

void MD5Init(MD5_CTX *);
void MD5Update(MD5_CTX *, unsigned char *, unsigned int);
void MD5Final(unsigned char [16], MD5_CTX *);
/* ==== end global.h extract ==== */

static NSString *hashSource(NSString *source) {
    MD5_CTX context;
    unsigned char hash[16];
    MD5Init(&context);
    MD5Update(&context, (unsigned char *)[source UTF8String], [source lengthOfBytesUsingEncoding:NSUTF8StringEncoding]);
    MD5Final(hash, &context);
    
    NSMutableString *hashString = [NSMutableString stringWithCapacity:32];
    for (unsigned i = 0; i < 16; i++)
        [hashString appendFormat:@"%02x", (unsigned int)hash[i]];
    return hashString;
}


@implementation CouchbaseCallbacks


+ (CouchbaseCallbacks*) sharedInstance {
    static dispatch_once_t onceToken;
    static CouchbaseCallbacks* sInstance;
    dispatch_once(&onceToken, ^{
        sInstance = [[self alloc] init];
    });
    return sInstance;
}


- (id)init {
    self = [super init];
    if (self) {
        NSMutableArray *mutableRegistries = [NSMutableArray arrayWithCapacity: kNumBlockTypes];
        for (int i=0; i<kNumBlockTypes; i++)
            [mutableRegistries addObject: [NSMutableDictionary dictionary]];
        _registries = [mutableRegistries copy];
    }
    return self;
}


- (void)dealloc {
    [_registries release];
    [super dealloc];
}


- (NSString*) generateKey {
    CFUUIDRef uuid = CFUUIDCreate(NULL);
    CFStringRef uuidStr = CFUUIDCreateString(NULL, uuid);
    CFRelease(uuid);
    return [NSMakeCollectable(uuidStr) autorelease];
}


- (void) registerBlock: (id)block ofType: (BlockType)type withVersionIdentifier: (NSString*)vId forKey: (NSString*)key {
    block = [block copy];
    id nonNilVersionId = vId ? : (id)[NSNull null];
    NSMutableDictionary* registry = [_registries objectAtIndex: type];
    NSDictionary* newEntry = [NSDictionary dictionaryWithObjectsAndKeys:
                              block, @"block", nonNilVersionId, @"versionIdentifier", nil];
    @synchronized(registry) {
        [registry setValue: newEntry forKey: key];
    }
    [block release];
}

- (NSDictionary*) registryEntryOfType: (BlockType)type forKey: (NSString*)key {
    NSMutableDictionary* registry = [_registries objectAtIndex: type];
    @synchronized(registry) {
        return [[[registry objectForKey: key] retain] autorelease];
    }
}

- (id) blockOfType: (BlockType)type forKey: (NSString*)key {
    return [[self registryEntryOfType:type forKey:key] objectForKey:@"block"];
}

- (NSString*) versionIdentifierForBlockOfType: (BlockType)type forKey: (NSString*)key {
    id vIdOrNull = [[self registryEntryOfType:type forKey:key] objectForKey:@"versionIdentifier"];
    if (vIdOrNull == [NSNull null]) return nil;
    return vIdOrNull;
}


- (void) registerMapBlock: (CouchMapBlock)block versionDependentString: (NSString*)source forKey: (NSString*)key {
    [self registerBlock: block ofType: kMapBlock withVersionIdentifier: hashSource(source) forKey: key];
}

- (CouchMapBlock) mapBlockForKey: (NSString*)key {
    return [self blockOfType: kMapBlock forKey: key];
}

- (NSString*) mapBlockVersionIdentifierForKey: (NSString*)key {
    return [self versionIdentifierForBlockOfType: kMapBlock forKey:key];
}


- (void) registerReduceBlock: (CouchReduceBlock)block versionDependentString: (NSString*)source forKey: (NSString*)key {
    [self registerBlock: block ofType: kReduceBlock withVersionIdentifier: hashSource(source) forKey: key];
}

- (CouchReduceBlock) reduceBlockForKey: (NSString*)key {
    return [self blockOfType: kReduceBlock forKey: key];
}

- (NSString*) reduceBlockVersionIdentifierForKey: (NSString*)key {
    return [self versionIdentifierForBlockOfType: kReduceBlock forKey:key];
}


- (void) registerValidateUpdateBlock: (CouchValidateUpdateBlock)block forKey: (NSString*)key {
    [self registerBlock: block ofType: kValidateUpdateBlock withVersionIdentifier: nil forKey: key];
}

- (CouchValidateUpdateBlock) validateUpdateBlockForKey: (NSString*)key {
    return [self blockOfType: kValidateUpdateBlock forKey: key];
}


@end
