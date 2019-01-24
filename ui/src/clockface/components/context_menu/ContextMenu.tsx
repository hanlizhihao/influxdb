// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'
// Components
import ContextMenuItem from 'src/clockface/components/context_menu/ContextMenuItem'
import {ClickOutside} from 'src/shared/components/ClickOutside'
import Button from 'src/clockface/components/Button'
// Types
import {ButtonShape, ComponentColor, ComponentSize, IconFont,} from 'src/clockface/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element | JSX.Element[]
  icon: IconFont
  text?: string
  title
  color?: ComponentColor
  shape?: ButtonShape
  onBoostZIndex?: (boostZIndex: boolean) => void
}

interface State {
  isExpanded: boolean
}

@ErrorHandling
class ContextMenu extends Component<Props, State> {
  public static defaultProps: Partial<Props> = {
    color: ComponentColor.Primary,
    shape: ButtonShape.Square,
    text: '',
  }

  constructor(props: Props) {
    super(props)

    this.state = {
      isExpanded: false,
    }
  }

  public render() {
    const {icon, text, shape, color} = this.props

    return (
      <ClickOutside onClickOutside={this.handleCollapseMenu}>
        <div className="context-menu--container">
          <Button
            customClass={this.toggleClassName}
            onClick={this.handleExpandMenu}
            text={text}
            shape={shape}
            icon={icon}
            size={ComponentSize.ExtraSmall}
            color={color}
          />
          {this.menu}
        </div>
      </ClickOutside>
    )
  }

  private handleExpandMenu = (): void => {
    const {onBoostZIndex} = this.props

    if (onBoostZIndex) {
      onBoostZIndex(true)
    }

    this.setState({isExpanded: true})
  }

  private handleCollapseMenu = (): void => {
    const {onBoostZIndex} = this.props

    if (onBoostZIndex) {
      onBoostZIndex(false)
    }

    this.setState({isExpanded: false})
  }

  private get menu(): JSX.Element {
    const {children} = this.props

    return (
      <div className={this.menuClassName}>
        <div className={this.listClassName}>
          {React.Children.map(children, (child: JSX.Element) => {
            if (child.type === ContextMenuItem) {
              return (
                <ContextMenuItem
                  {...child.props}
                  onCollapseMenu={this.handleCollapseMenu}
                />
              )
            } else {
              throw new Error('Expected children of type <Context.Item />')
            }
          })}
        </div>
      </div>
    )
  }

  private get listClassName(): string {
    const {color} = this.props

    return classnames('context-menu--list', {
      [`context-menu--${color}`]: color,
    })
  }

  private get menuClassName(): string {
    const {isExpanded} = this.state

    return classnames('context-menu--list-container', {open: isExpanded})
  }

  private get toggleClassName(): string {
    const {color} = this.props
    const {isExpanded} = this.state

    return classnames('context-menu--toggle', {
      [`context-menu--${color}`]: color,
      active: isExpanded,
    })
  }
}

export default ContextMenu
