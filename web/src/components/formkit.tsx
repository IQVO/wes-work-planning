import type { ChangeEvent, ReactNode } from "react";

/**
 * Minimal styled form primitives, copied from order-management's (and
 * facility-mfe's) components/formkit.tsx -- not promoted to
 * @warehouse/ui-kit, every remote's copy is allowed to diverge slightly.
 * Styled purely off the shared design tokens so it looks native inside the
 * console shell.
 */

const fieldWrapStyle = {
  display: "flex",
  flexDirection: "column" as const,
  gap: 4,
};

const labelStyle = {
  fontSize: "var(--wh-font-size-xs)",
  color: "var(--wh-color-text-muted)",
  fontWeight: 600,
};

const inputStyle = {
  padding: "8px 10px",
  borderRadius: "var(--wh-radius-md)",
  border: "1px solid var(--wh-color-border)",
  background: "var(--wh-color-bg-sunken)",
  color: "var(--wh-color-text)",
  fontFamily: "var(--wh-font-mono)",
  fontSize: "var(--wh-font-size-sm)",
};

export function TextField({
  label,
  value,
  onChange,
  placeholder,
  type = "text",
  hint,
  required,
  disabled,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  type?: "text" | "number" | "datetime-local";
  hint?: string;
  required?: boolean;
  disabled?: boolean;
}) {
  return (
    <label style={fieldWrapStyle}>
      <span style={labelStyle}>
        {label}
        {required && " *"}
      </span>
      <input
        type={type}
        value={value}
        placeholder={placeholder}
        disabled={disabled}
        onChange={(e: ChangeEvent<HTMLInputElement>) => onChange(e.target.value)}
        style={{ ...inputStyle, opacity: disabled ? 0.6 : 1 }}
      />
      {hint && (
        <span style={{ fontSize: "var(--wh-font-size-xs)", color: "var(--wh-color-text-faint)" }}>
          {hint}
        </span>
      )}
    </label>
  );
}

export function SelectField<T extends string>({
  label,
  value,
  options,
  onChange,
  hint,
  required,
  disabled,
}: {
  label: string;
  value: T;
  options: readonly T[];
  onChange: (value: T) => void;
  hint?: string;
  required?: boolean;
  disabled?: boolean;
}) {
  return (
    <label style={fieldWrapStyle}>
      <span style={labelStyle}>
        {label}
        {required && " *"}
      </span>
      <select
        value={value}
        disabled={disabled}
        onChange={(e: ChangeEvent<HTMLSelectElement>) => onChange(e.target.value as T)}
        style={{ ...inputStyle, opacity: disabled ? 0.6 : 1 }}
      >
        {options.map((option) => (
          <option key={option} value={option}>
            {option}
          </option>
        ))}
      </select>
      {hint && (
        <span style={{ fontSize: "var(--wh-font-size-xs)", color: "var(--wh-color-text-faint)" }}>
          {hint}
        </span>
      )}
    </label>
  );
}

export function FormRow({ children }: { children: ReactNode }) {
  return (
    <div style={{ display: "flex", gap: "var(--wh-space-3)", flexWrap: "wrap", alignItems: "end" }}>
      {children}
    </div>
  );
}

export function SubmitButton({
  children,
  disabled,
  onClick,
  type = "submit",
}: {
  children: ReactNode;
  disabled?: boolean;
  /** Set together with type="button" to use this as a plain action button
   *  outside a <form> submit flow (e.g. removing a bucket row). */
  onClick?: () => void;
  type?: "submit" | "button";
}) {
  const bg = disabled ? "var(--wh-color-border)" : "var(--wh-color-accent)";
  return (
    <button
      type={type}
      onClick={onClick}
      disabled={disabled}
      style={{
        padding: "9px 16px",
        borderRadius: "var(--wh-radius-md)",
        border: "none",
        background: bg,
        color: "#fff",
        fontWeight: 600,
        fontSize: "var(--wh-font-size-sm)",
        cursor: disabled ? "not-allowed" : "pointer",
        height: 36,
      }}
    >
      {children}
    </button>
  );
}

export function InlineSuccess({ message }: { message: string | null }) {
  if (!message) return null;
  return (
    <div
      style={{
        color: "var(--wh-color-status-success)",
        background: "var(--wh-color-status-success-bg)",
        borderRadius: "var(--wh-radius-md)",
        padding: "8px 12px",
        fontSize: "var(--wh-font-size-sm)",
      }}
    >
      {message}
    </div>
  );
}
