package mysql

import (
	"crypto/sha1"
	"crypto/sha256"
)

// 关于 caching_sha2_password 可以参考：
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_caching_sha2_authentication_exchanges.html#sect_caching_sha2_info
// XOR(SHA256(password), SHA256(SHA256(SHA256(password)), scramble))

func buildAuthRespWithCachingSha2Password(scramble []byte, password string) []byte {
	hash := sha256.New()
	hash.Write([]byte(password))
	msg1 := hash.Sum(nil)

	hash.Reset()
	hash.Write(msg1)
	msg2 := hash.Sum(nil)

	hash.Reset()
	hash.Write(msg2)
	hash.Write(scramble)
	msg3 := hash.Sum(nil)

	for i := 0; i < len(msg1); i++ {
		msg1[i] ^= msg3[i]
	}

	return msg1
}

// 关于 mysql_native_password 可以参考：
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_authentication_methods_native_password_authentication.html
// SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1( SHA1( password ) ) )

func buildAuthRespWithMysqlNativePassword(scramble []byte, password string) []byte {
	hash := sha1.New()
	hash.Write([]byte(password))
	msg1 := hash.Sum(nil)

	hash.Reset()
	hash.Write(msg1)
	msg2 := hash.Sum(nil)

	hash.Reset()
	hash.Write(scramble)
	hash.Write(msg2)
	msg3 := hash.Sum(nil)

	for idx := 0; idx < len(msg1); idx++ {
		msg1[idx] ^= msg3[idx]
	}

	return msg1
}
