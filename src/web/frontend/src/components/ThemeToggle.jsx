export function ThemeToggle({ theme, onToggle }) {
  return (
    <button
      id="theme-toggle-btn"
      className="theme-toggle"
      onClick={onToggle}
      title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}
    >
      {theme === 'dark' ? '☀️' : '🌙'}
      <span>{theme === 'dark' ? 'Light' : 'Dark'}</span>
    </button>
  )
}
