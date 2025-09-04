// Command Panel (⌘/Ctrl+K) - Cyber Command Center
class CommandPanel {
  constructor() {
    this.isOpen = false;
    this.commands = new Map();
    this.filteredCommands = [];
    this.selectedIndex = 0;
    this.element = null;
    
    this.init();
    this.registerDefaultCommands();
  }

  init() {
    // Listen for keyboard shortcuts
    document.addEventListener('keydown', (e) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault();
        this.toggle();
      } else if (e.key === 'Escape' && this.isOpen) {
        this.close();
      }
    });

    // Create command panel element
    this.createElement();
  }

  createElement() {
    const panel = document.createElement('div');
    panel.id = 'command-panel';
    panel.className = 'command-panel';
    panel.innerHTML = `
      <div class="command-panel-backdrop"></div>
      <div class="command-panel-content">
        <div class="command-search-container">
          <input 
            type="text" 
            id="command-search" 
            class="command-search-input" 
            placeholder="搜索命令或设备..."
            autocomplete="off"
          />
          <div class="command-shortcut-hint">⌘K</div>
        </div>
        <div class="command-results" id="command-results">
          <div class="command-category">
            <div class="command-category-title">快捷操作</div>
            <div class="command-category-items" id="quick-actions"></div>
          </div>
          <div class="command-category">
            <div class="command-category-title">导航</div>
            <div class="command-category-items" id="navigation"></div>
          </div>
          <div class="command-category">
            <div class="command-category-title">设备操作</div>
            <div class="command-category-items" id="device-actions"></div>
          </div>
        </div>
      </div>
    `;

    document.body.appendChild(panel);
    this.element = panel;

    // Add event listeners
    const searchInput = panel.querySelector('#command-search');
    const backdrop = panel.querySelector('.command-panel-backdrop');

    searchInput.addEventListener('input', (e) => this.handleSearch(e.target.value));
    searchInput.addEventListener('keydown', (e) => this.handleKeydown(e));
    backdrop.addEventListener('click', () => this.close());
  }

  registerDefaultCommands() {
    // Quick Actions
    this.addCommand({
      id: 'export-dashboard',
      title: '导出仪表盘数据',
      description: '导出当前仪表盘所有指标数据',
      category: 'quick-actions',
      icon: '⬇️',
      action: () => this.exportDashboard(),
      keywords: ['导出', 'export', '数据', 'data']
    });

    this.addCommand({
      id: 'refresh-all',
      title: '刷新所有面板',
      description: '重新加载所有仪表盘面板数据',
      category: 'quick-actions',
      icon: '🔄',
      action: () => this.refreshAll(),
      keywords: ['刷新', 'refresh', '重载', 'reload']
    });

    this.addCommand({
      id: 'toggle-theme',
      title: '切换主题',
      description: '在明暗主题间切换',
      category: 'quick-actions',
      icon: '🌙',
      action: () => cmUI.toggleTheme(),
      keywords: ['主题', 'theme', '暗色', 'dark', '明亮', 'light']
    });

    this.addCommand({
      id: 'command-center',
      title: '批量命令中心',
      description: '打开批量命令操作面板',
      category: 'quick-actions',
      icon: '⚡',
      action: () => this.openCommandCenter(),
      keywords: ['命令', 'command', '批量', 'batch']
    });

    // Navigation
    this.addCommand({
      id: 'goto-devices',
      title: '设备管理',
      description: '跳转到设备管理页面',
      category: 'navigation',
      icon: '📱',
      action: () => window.location.href = '/devices',
      keywords: ['设备', 'device', 'machine']
    });

    this.addCommand({
      id: 'goto-orders',
      title: '订单管理',
      description: '查看所有订单记录',
      category: 'navigation',
      icon: '📋',
      action: () => window.location.href = '/orders',
      keywords: ['订单', 'order', '销售', 'sales']
    });

    this.addCommand({
      id: 'goto-dispatch',
      title: '下发中心',
      description: '管理命令下发和批次',
      category: 'navigation',
      icon: '🚀',
      action: () => window.location.href = '/dispatch',
      keywords: ['下发', 'dispatch', '命令', 'command']
    });

    this.addCommand({
      id: 'goto-alarms',
      title: '告警管理',
      description: '查看和处理系统告警',
      category: 'navigation',
      icon: '🚨',
      action: () => window.location.href = '/alarms',
      keywords: ['告警', 'alarm', '警报', 'alert']
    });

    // Device Actions
    this.addCommand({
      id: 'sync-all-devices',
      title: '同步所有设备状态',
      description: '强制同步所有设备的最新状态',
      category: 'device-actions',
      icon: '🔄',
      action: () => this.syncAllDevices(),
      keywords: ['同步', 'sync', '设备', 'device', '状态', 'status']
    });

    this.addCommand({
      id: 'reboot-devices',
      title: '重启离线设备',
      description: '发送重启命令到所有离线设备',
      category: 'device-actions',
      icon: '🔄',
      action: () => this.rebootOfflineDevices(),
      keywords: ['重启', 'reboot', '离线', 'offline']
    });
  }

  addCommand(command) {
    this.commands.set(command.id, command);
  }

  removeCommand(id) {
    this.commands.delete(id);
  }

  toggle() {
    if (this.isOpen) {
      this.close();
    } else {
      this.open();
    }
  }

  open() {
    if (this.isOpen) return;
    
    this.isOpen = true;
    this.element.classList.add('active');
    
    // Focus search input
    setTimeout(() => {
      const searchInput = this.element.querySelector('#command-search');
      searchInput.focus();
      searchInput.select();
    }, 100);

    // Show default commands
    this.showDefaultCommands();
  }

  close() {
    if (!this.isOpen) return;
    
    this.isOpen = false;
    this.element.classList.remove('active');
    
    // Clear search
    const searchInput = this.element.querySelector('#command-search');
    searchInput.value = '';
    this.selectedIndex = 0;
  }

  handleSearch(query) {
    const trimmedQuery = query.trim().toLowerCase();
    
    if (!trimmedQuery) {
      this.showDefaultCommands();
      return;
    }

    // Filter commands by query
    this.filteredCommands = Array.from(this.commands.values()).filter(command => {
      const matchTitle = command.title.toLowerCase().includes(trimmedQuery);
      const matchDescription = command.description.toLowerCase().includes(trimmedQuery);
      const matchKeywords = command.keywords?.some(keyword => 
        keyword.toLowerCase().includes(trimmedQuery)
      );
      
      return matchTitle || matchDescription || matchKeywords;
    });

    this.renderFilteredCommands();
    this.selectedIndex = 0;
    this.updateSelection();
  }

  handleKeydown(e) {
    const resultItems = this.element.querySelectorAll('.command-item');
    
    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault();
        this.selectedIndex = Math.min(this.selectedIndex + 1, resultItems.length - 1);
        this.updateSelection();
        break;
        
      case 'ArrowUp':
        e.preventDefault();
        this.selectedIndex = Math.max(this.selectedIndex - 1, 0);
        this.updateSelection();
        break;
        
      case 'Enter':
        e.preventDefault();
        const selectedItem = resultItems[this.selectedIndex];
        if (selectedItem) {
          selectedItem.click();
        }
        break;
        
      case 'Escape':
        this.close();
        break;
    }
  }

  updateSelection() {
    const resultItems = this.element.querySelectorAll('.command-item');
    resultItems.forEach((item, index) => {
      item.classList.toggle('selected', index === this.selectedIndex);
    });
    
    // Scroll selected item into view
    const selectedItem = resultItems[this.selectedIndex];
    if (selectedItem) {
      selectedItem.scrollIntoView({ block: 'nearest' });
    }
  }

  showDefaultCommands() {
    // Group commands by category
    const categories = {
      'quick-actions': [],
      'navigation': [],
      'device-actions': []
    };

    this.commands.forEach(command => {
      if (categories[command.category]) {
        categories[command.category].push(command);
      }
    });

    // Render each category
    Object.keys(categories).forEach(categoryId => {
      const container = this.element.querySelector(`#${categoryId}`);
      container.innerHTML = '';
      
      categories[categoryId].forEach(command => {
        const commandElement = this.createCommandElement(command);
        container.appendChild(commandElement);
      });
    });

    this.filteredCommands = Array.from(this.commands.values());
  }

  renderFilteredCommands() {
    const resultsContainer = this.element.querySelector('#command-results');
    resultsContainer.innerHTML = '';

    if (this.filteredCommands.length === 0) {
      resultsContainer.innerHTML = `
        <div class="command-no-results">
          <div class="no-results-icon">🔍</div>
          <div class="no-results-text">未找到匹配的命令</div>
        </div>
      `;
      return;
    }

    // Group filtered commands by category
    const categories = {};
    this.filteredCommands.forEach(command => {
      if (!categories[command.category]) {
        categories[command.category] = [];
      }
      categories[command.category].push(command);
    });

    // Render categories
    Object.keys(categories).forEach(categoryId => {
      const categoryDiv = document.createElement('div');
      categoryDiv.className = 'command-category';
      
      const categoryTitle = document.createElement('div');
      categoryTitle.className = 'command-category-title';
      categoryTitle.textContent = this.getCategoryTitle(categoryId);
      
      const categoryItems = document.createElement('div');
      categoryItems.className = 'command-category-items';
      
      categories[categoryId].forEach(command => {
        const commandElement = this.createCommandElement(command);
        categoryItems.appendChild(commandElement);
      });
      
      categoryDiv.appendChild(categoryTitle);
      categoryDiv.appendChild(categoryItems);
      resultsContainer.appendChild(categoryDiv);
    });
  }

  createCommandElement(command) {
    const div = document.createElement('div');
    div.className = 'command-item';
    div.innerHTML = `
      <div class="command-icon">${command.icon}</div>
      <div class="command-content">
        <div class="command-title">${command.title}</div>
        <div class="command-description">${command.description}</div>
      </div>
    `;
    
    div.addEventListener('click', () => {
      command.action();
      this.close();
    });
    
    return div;
  }

  getCategoryTitle(categoryId) {
    const titles = {
      'quick-actions': '快捷操作',
      'navigation': '导航',
      'device-actions': '设备操作'
    };
    return titles[categoryId] || categoryId;
  }

  // Action implementations
  exportDashboard() {
    if (typeof exportSummary === 'function') {
      exportSummary();
    } else {
      cmUI.toast('导出功能暂不可用', 'warning');
    }
  }

  refreshAll() {
    if (typeof reloadAll === 'function') {
      reloadAll();
      cmUI.toast('正在刷新所有面板...', 'info');
    } else {
      location.reload();
    }
  }

  openCommandCenter() {
    window.location.href = '/dispatch';
  }

  async syncAllDevices() {
    try {
      const response = await fetch('/api/v1/devices/sync-all', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });
      
      if (response.ok) {
        cmUI.toast('设备状态同步已启动', 'success');
      } else {
        cmUI.toast('同步请求失败', 'error');
      }
    } catch (error) {
      cmUI.toast('网络错误', 'error');
    }
  }

  async rebootOfflineDevices() {
    const confirmed = await cmUI.confirmDialog({
      title: '确认重启',
      message: '确定要重启所有离线设备吗？此操作可能需要几分钟完成。'
    });
    
    if (confirmed) {
      try {
        const response = await fetch('/api/v1/devices/reboot-offline', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' }
        });
        
        if (response.ok) {
          cmUI.toast('重启命令已发送', 'success');
        } else {
          cmUI.toast('重启命令发送失败', 'error');
        }
      } catch (error) {
        cmUI.toast('网络错误', 'error');
      }
    }
  }
}

// Initialize command panel when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  window.commandPanel = new CommandPanel();
});