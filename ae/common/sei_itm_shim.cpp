#include <cstddef>

extern "C" {
void* _ITM_memcpyRtWt(void* dst, const void* src, std::size_t size);
void* _ITM_memmoveRtWt(void* dst, const void* src, std::size_t size);
void* _ITM_memsetW(void* dst, int c, std::size_t size);
void _ITM_commitTransaction(void);

void _ITM_commitTransactionEH(void) { _ITM_commitTransaction(); }

void* _ITM_memcpyRnWt(void* dst, const void* src, std::size_t size) {
    return _ITM_memcpyRtWt(dst, src, size);
}
void* _ITM_memcpyRnWtaR(void* dst, const void* src, std::size_t size) {
    return _ITM_memcpyRtWt(dst, src, size);
}
void* _ITM_memcpyRnWtaW(void* dst, const void* src, std::size_t size) {
    return _ITM_memcpyRtWt(dst, src, size);
}
void* _ITM_memcpyRtWn(void* dst, const void* src, std::size_t size) {
    return _ITM_memcpyRtWt(dst, src, size);
}
void* _ITM_memcpyRtWtaR(void* dst, const void* src, std::size_t size) {
    return _ITM_memcpyRtWt(dst, src, size);
}
void* _ITM_memcpyRtWtaW(void* dst, const void* src, std::size_t size) {
    return _ITM_memcpyRtWt(dst, src, size);
}
void* _ITM_memcpyRtaRWn(void* dst, const void* src, std::size_t size) {
    return _ITM_memcpyRtWt(dst, src, size);
}
void* _ITM_memcpyRtaRWt(void* dst, const void* src, std::size_t size) {
    return _ITM_memcpyRtWt(dst, src, size);
}
void* _ITM_memcpyRtaRWtaR(void* dst, const void* src, std::size_t size) {
    return _ITM_memcpyRtWt(dst, src, size);
}
void* _ITM_memcpyRtaRWtaW(void* dst, const void* src, std::size_t size) {
    return _ITM_memcpyRtWt(dst, src, size);
}
void* _ITM_memcpyRtaWWn(void* dst, const void* src, std::size_t size) {
    return _ITM_memcpyRtWt(dst, src, size);
}
void* _ITM_memcpyRtaWWt(void* dst, const void* src, std::size_t size) {
    return _ITM_memcpyRtWt(dst, src, size);
}
void* _ITM_memcpyRtaWWtaR(void* dst, const void* src, std::size_t size) {
    return _ITM_memcpyRtWt(dst, src, size);
}
void* _ITM_memcpyRtaWWtaW(void* dst, const void* src, std::size_t size) {
    return _ITM_memcpyRtWt(dst, src, size);
}

void* _ITM_memmoveRnWt(void* dst, const void* src, std::size_t size) {
    return _ITM_memmoveRtWt(dst, src, size);
}
void* _ITM_memmoveRnWtaR(void* dst, const void* src, std::size_t size) {
    return _ITM_memmoveRtWt(dst, src, size);
}
void* _ITM_memmoveRnWtaW(void* dst, const void* src, std::size_t size) {
    return _ITM_memmoveRtWt(dst, src, size);
}
void* _ITM_memmoveRtWn(void* dst, const void* src, std::size_t size) {
    return _ITM_memmoveRtWt(dst, src, size);
}
void* _ITM_memmoveRtWtaR(void* dst, const void* src, std::size_t size) {
    return _ITM_memmoveRtWt(dst, src, size);
}
void* _ITM_memmoveRtWtaW(void* dst, const void* src, std::size_t size) {
    return _ITM_memmoveRtWt(dst, src, size);
}
void* _ITM_memmoveRtaRWn(void* dst, const void* src, std::size_t size) {
    return _ITM_memmoveRtWt(dst, src, size);
}
void* _ITM_memmoveRtaRWt(void* dst, const void* src, std::size_t size) {
    return _ITM_memmoveRtWt(dst, src, size);
}
void* _ITM_memmoveRtaRWtaR(void* dst, const void* src, std::size_t size) {
    return _ITM_memmoveRtWt(dst, src, size);
}
void* _ITM_memmoveRtaRWtaW(void* dst, const void* src, std::size_t size) {
    return _ITM_memmoveRtWt(dst, src, size);
}
void* _ITM_memmoveRtaWWn(void* dst, const void* src, std::size_t size) {
    return _ITM_memmoveRtWt(dst, src, size);
}
void* _ITM_memmoveRtaWWt(void* dst, const void* src, std::size_t size) {
    return _ITM_memmoveRtWt(dst, src, size);
}
void* _ITM_memmoveRtaWWtaR(void* dst, const void* src, std::size_t size) {
    return _ITM_memmoveRtWt(dst, src, size);
}
void* _ITM_memmoveRtaWWtaW(void* dst, const void* src, std::size_t size) {
    return _ITM_memmoveRtWt(dst, src, size);
}

void* _ITM_memsetWaR(void* dst, int c, std::size_t size) {
    return _ITM_memsetW(dst, c, size);
}
void* _ITM_memsetWaW(void* dst, int c, std::size_t size) {
    return _ITM_memsetW(dst, c, size);
}
}

