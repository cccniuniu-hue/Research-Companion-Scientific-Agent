// Package skill 是「Skill 广场」的应用服务层：编排广场展示、安装/卸载/开关，
// 并把已安装且开启的 skill 转换成 domain/tool.Tool 供主 Agent 循环调用。
//
// 依赖方向：application/skill 依赖 infrastructure（skillrepo / skillhub / llm / toolimpl），
// 但 application/chat 不依赖本包——通过 main.go 注入 EnabledTools 闭包解耦。
package skill

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strings"

	skilldomain "agi-assistant/internal/domain/skill"
	"agi-assistant/internal/domain/tool"
	"agi-assistant/internal/infrastructure/llm"
	"agi-assistant/internal/infrastructure/persistence/skillrepo"
	"agi-assistant/internal/infrastructure/skillhub"
	toolimpl "agi-assistant/internal/infrastructure/tool"
	"agi-assistant/internal/pkg/logger"
	"agi-assistant/internal/usercontext"
)

// ErrManifestNotFound 表示广场（内置 + GitHub 缓存）中找不到该 skillID 对应的 manifest。
var ErrManifestNotFound = errors.New("skill manifest not found in marketplace")

// Service 聚合 skill 广场的全部用例。
type Service struct {
	repo    skillrepo.Repo
	hub     *skillhub.Client // 可为 nil（广场禁用 GitHub 时）
	llm     *llm.Client
	enabled bool // GitHub 广场开关
}

// NewService 创建 skill 应用服务。hub==nil 或 hubEnabled==false 时只提供内置精选。
func NewService(repo skillrepo.Repo, hub *skillhub.Client, llmClient *llm.Client, hubEnabled bool) *Service {
	return &Service{repo: repo, hub: hub, llm: llmClient, enabled: hubEnabled}
}

// Marketplace 返回广场数据：内置精选 + GitHub 热门。
// hubStatus 为 "ok" / "disabled" / "degraded"，供前端提示。
func (s *Service) Marketplace(ctx context.Context) (featured, github []skilldomain.Manifest, hubStatus string) {
	featured = skillhub.BuiltinOfficeSkills()

	if !s.enabled || s.hub == nil {
		return featured, nil, "disabled"
	}
	items, err := s.hub.SearchOfficeSkills(ctx)
	if err != nil {
		logger.C(ctx).Warn("skill marketplace github degraded", "err", err)
		return featured, nil, "degraded"
	}
	return featured, items, "ok"
}

// ListInstalled 返回用户已安装的 skill（含开关状态）。
func (s *Service) ListInstalled(userID string) ([]skilldomain.Skill, error) {
	if err := s.requireRepo(userID); err != nil {
		return nil, err
	}
	return s.repo.ListByUser(userID)
}

// Install 按 skillID 在广场（内置 + GitHub 缓存）中查到 manifest 后落库。
// 用 skillID 反查而非信任前端传入的完整 manifest，避免伪造/注入。
func (s *Service) Install(ctx context.Context, userID, skillID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.requireRepo(userID); err != nil {
		return err
	}
	m, ok := s.findManifest(ctx, skillID)
	if err := ctx.Err(); err != nil {
		return err
	}
	if !ok {
		return ErrManifestNotFound
	}
	sanitizeManifest(&m)
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.repo.Install(userID, m)
}

// Uninstall 卸载一条 skill。
func (s *Service) Uninstall(userID, skillID string) (bool, error) {
	if err := s.requireRepo(userID); err != nil {
		return false, err
	}
	return s.repo.Uninstall(userID, skillID)
}

// Toggle 切换 skill 开关。
func (s *Service) Toggle(userID, skillID string, enabled bool) (bool, error) {
	if err := s.requireRepo(userID); err != nil {
		return false, err
	}
	return s.repo.SetEnabled(userID, skillID, enabled)
}

// EnabledTools 返回用户已开启 skill 对应的工具集，供主 Agent 循环合并。
// 这是「开关开启 → 主循环可扫描」的落点：只有 enabled=TRUE 的 skill 才在此出现。
func (s *Service) EnabledTools(userID string) map[string]tool.Tool {
	if err := s.requireRepo(userID); err != nil {
		return nil
	}
	skills, err := s.repo.ListEnabled(userID)
	if err != nil {
		logger.L().Warn("skill: list enabled failed", "user", userID, "err", err)
		return nil
	}
	if len(skills) == 0 {
		return nil
	}
	out := make(map[string]tool.Tool, len(skills))
	ambiguous := make(map[string]bool)
	for _, sk := range skills {
		if sk.UserID != userID || !sk.Enabled || !validManifest(sk.Manifest) {
			continue
		}
		t := s.toTool(sk)
		if _, exists := out[t.Name]; exists || ambiguous[t.Name] {
			delete(out, t.Name)
			ambiguous[t.Name] = true
			continue
		}
		out[t.Name] = t
	}
	names := make([]string, 0, len(out))
	for name := range out {
		names = append(names, name)
	}
	sort.Strings(names)
	logger.L().Info("skill: enabled tools merged into loop", "user", userID, "count", len(out), "names", names)
	return out
}

func (s *Service) requireRepo(userID string) error {
	if strings.TrimSpace(userID) == "" {
		return skillrepo.ErrInvalidUser
	}
	if s.repo == nil {
		return skillrepo.ErrUnavailable
	}
	return nil
}

func validManifest(m skilldomain.Manifest) bool {
	if strings.TrimSpace(m.ID) == "" {
		return false
	}
	switch m.Invocation {
	case skilldomain.InvokePrompt:
		return true
	case skilldomain.InvokeMCP:
		u, err := url.Parse(m.Endpoint)
		return err == nil && u.Host != "" && u.User == nil && (u.Scheme == "http" || u.Scheme == "https")
	default:
		return false
	}
}

// findManifest 在内置目录与 GitHub 缓存中按 ID 查找 manifest。
func (s *Service) findManifest(ctx context.Context, skillID string) (skilldomain.Manifest, bool) {
	for _, m := range skillhub.BuiltinOfficeSkills() {
		if m.ID == skillID {
			return m, true
		}
	}
	if s.enabled && s.hub != nil {
		if items, err := s.hub.SearchOfficeSkills(ctx); err == nil {
			for _, m := range items {
				if m.ID == skillID {
					return m, true
				}
			}
		}
	}
	return skilldomain.Manifest{}, false
}

// toTool 把已安装 skill 转成可执行工具。
//   - InvokeMCP：复用 NewMCPTool 调外部 HTTP 端点
//   - InvokePrompt：本地 LLM 按 PromptTemplate 执行（默认）
func (s *Service) toTool(sk skilldomain.Skill) tool.Tool {
	sk.Parameters = slices.Clone(sk.Parameters)
	if sk.Invocation == skilldomain.InvokeMCP && sk.Endpoint != "" {
		return bindSkillOwner(sk.UserID, toolimpl.NewMCPTool(sk.ToolName(), sk.Description, sk.Endpoint, sk.Parameters))
	}

	tmpl := sk.PromptTemplate
	if tmpl == "" {
		tmpl = "{{input}}"
	}
	name := sk.ToolName()
	desc := sk.Description
	params := sk.Parameters
	if len(params) == 0 {
		params = []tool.Param{{Name: "input", Type: "string", Description: "任务输入", Required: true}}
	}

	render := func(p map[string]interface{}) (string, error) {
		input, _ := p["input"].(string)
		if _, explicit := p["input"]; !explicit {
			keys := make([]string, 0, len(p))
			for key := range p {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				if sv, ok := p[key].(string); ok && strings.TrimSpace(sv) != "" {
					input = sv
					break
				}
			}
		}
		if strings.TrimSpace(input) == "" {
			return "", fmt.Errorf("skill input must be a nonempty string")
		}
		if !strings.Contains(tmpl, "{{input}}") {
			return tmpl + "\n\n任务输入：\n" + input, nil
		}
		return strings.ReplaceAll(tmpl, "{{input}}", input), nil
	}
	const sysPrompt = "你是专业的办公助手，请严格按照用户任务完成办公工作，输出简洁、结构清晰、可直接使用。"

	return bindSkillOwner(sk.UserID, tool.Tool{
		Name:        name,
		Description: desc,
		Parameters:  params,
		ExecuteCtx: func(ctx context.Context, p map[string]interface{}) (string, error) {
			if s.llm == nil {
				return "", fmt.Errorf("skill model client not configured")
			}
			prompt, err := render(p)
			if err != nil {
				return "", err
			}
			logger.C(ctx).Info("skill: invoked (execute-ctx)", "skill", name)
			out := s.llm.ChatContextFast(ctx, sysPrompt, []llm.Message{{Role: "user", Content: prompt}})
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			if out == "[已中断]" {
				return "", fmt.Errorf("skill %s interrupted", name)
			}
			return out, nil
		},
	})
}

// shortcut: legacy Execute binds the owner without caller context; never share closures across users.
func bindSkillOwner(owner string, t tool.Tool) tool.Tool {
	guard := func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if owner == "" || usercontext.UserIDFromContext(ctx) != owner {
			return fmt.Errorf("skill execution user does not match installation owner")
		}
		return nil
	}
	if call := t.ExecuteCtx; call != nil {
		t.ExecuteCtx = func(ctx context.Context, params map[string]interface{}) (string, error) {
			if err := guard(ctx); err != nil {
				return "", err
			}
			return call(ctx, params)
		}
		t.Execute = func(params map[string]interface{}) (string, error) {
			return t.ExecuteCtx(usercontext.With(context.Background(), owner, ""), params)
		}
	}
	if call := t.ExecuteStructured; call != nil {
		t.ExecuteStructured = func(ctx context.Context, params map[string]interface{}) tool.ToolResult {
			if err := guard(ctx); err != nil {
				code := "scope"
				if ctx.Err() != nil {
					code = "cancelled"
				}
				if errors.Is(err, context.DeadlineExceeded) {
					code = "timeout"
				}
				return tool.ToolResult{Error: &tool.ToolError{Code: code, Message: err.Error(), Cause: err}}
			}
			return call(ctx, params)
		}
	}
	return t
}

// sanitizeManifest 对入库前的 manifest 做防御性截断，避免超长描述 / 模板污染。
func sanitizeManifest(m *skilldomain.Manifest) {
	m.Description = truncate(m.Description, 500)
	m.PromptTemplate = truncate(m.PromptTemplate, 4000)
	m.Name = truncate(m.Name, 200)
}

func truncate(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}
