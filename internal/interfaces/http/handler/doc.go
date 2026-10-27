// Package handler exposes authenticated HTTP APIs for chat, memory and Skills.
//
// Registration/login and health probes are public. Other API routes require a
// verified JWT. Shared documents/RAG, global cancellation/snapshots/status,
// memory audit/mutations and global MCP registration also require X-Admin-Token,
// configured through PprofAdminToken even when pprof is disabled. Empty tokens
// disable management access. Ordinary memory reads contain only the owner’s
// active records; shared RAG is unavailable to ordinary chat requests.
//
// JSON APIs accept one known-field value up to 1 MiB; multipart upload is limited
// to 64 MiB. SSE writes and flush errors cancel execution. Shared knowledge
// operations are serialized per Server; direct core RAG calls remain the caller’s
// synchronization responsibility. Synchronous core work cannot be rolled back.
package handler
