// Libraries
import _ from 'lodash'
import React, {ChangeEvent, PureComponent} from 'react'
// Utils
import {downloadTextFile} from 'src/shared/utils/download'
// Components
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import CollectorList from 'src/organizations/components/CollectorList'
import TelegrafExplainer from 'src/organizations/components/TelegrafExplainer'
import {
    Button,
    Columns,
    ComponentColor,
    ComponentSize,
    EmptyState,
    Grid,
    IconFont,
    Input,
    InputType,
} from 'src/clockface'
import DataLoadersWizard from 'src/dataLoaders/components/DataLoadersWizard'
import FilterList from 'src/shared/components/Filter'
// APIS
import {deleteTelegrafConfig, getTelegrafConfigTOML,} from 'src/organizations/apis/index'
// Actions
import * as NotificationsActions from 'src/types/actions/notifications'
import {notify} from 'src/shared/actions/notifications'
// Constants
import {getTelegrafConfigFailed} from 'src/shared/copy/v2/notifications'
// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
// Types
import {Bucket, Telegraf} from 'src/api'
import {DataLoaderStep, DataLoaderType} from 'src/types/v2/dataLoaders'
import {OverlayState} from 'src/types/v2'

interface Props {
  collectors: Telegraf[]
  onChange: () => void
  notify: NotificationsActions.PublishNotificationActionCreator
  orgName: string
  buckets: Bucket[]
}

interface State {
  overlayState: OverlayState
  searchTerm: string
}

@ErrorHandling
export default class Collectors extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      overlayState: OverlayState.Closed,
      searchTerm: '',
    }
  }

  public render() {
    const {collectors, buckets} = this.props
    const {searchTerm} = this.state

    return (
      <>
        <TabbedPageHeader>
          <Input
            icon={IconFont.Search}
            placeholder="Filter telegraf configs by bucket..."
            widthPixels={290}
            value={searchTerm}
            type={InputType.Text}
            onChange={this.handleFilterChange}
            onBlur={this.handleFilterBlur}
          />
          {this.createButton}
        </TabbedPageHeader>
        <Grid>
          <Grid.Row>
            <Grid.Column widthSM={Columns.Twelve}>
              <FilterList<Telegraf>
                searchTerm={searchTerm}
                searchKeys={['plugins.0.config.bucket']}
                list={collectors}
              >
                {cs => (
                  <CollectorList
                    collectors={cs}
                    emptyState={this.emptyState}
                    onDownloadConfig={this.handleDownloadConfig}
                    onDelete={this.handleDeleteTelegraf}
                  />
                )}
              </FilterList>
            </Grid.Column>
            <Grid.Column
              widthSM={Columns.Six}
              widthMD={Columns.Four}
              offsetSM={Columns.Three}
              offsetMD={Columns.Four}
            >
              <TelegrafExplainer />
            </Grid.Column>
          </Grid.Row>
        </Grid>
        <DataLoadersWizard
          visible={this.isOverlayVisible}
          onCompleteSetup={this.handleDismissDataLoaders}
          startingType={DataLoaderType.Streaming}
          startingStep={DataLoaderStep.Select}
          startingSubstep={'streaming'}
          buckets={buckets}
        />
      </>
    )
  }

  private get isOverlayVisible(): boolean {
    return this.state.overlayState === OverlayState.Open
  }

  private get createButton(): JSX.Element {
    return (
      <Button
        text="Create Configuration"
        icon={IconFont.Plus}
        color={ComponentColor.Primary}
        onClick={this.handleAddScraper}
      />
    )
  }

  private handleAddScraper = () => {
    this.setState({overlayState: OverlayState.Open})
  }

  private handleDismissDataLoaders = () => {
    this.setState({overlayState: OverlayState.Closed})
    this.props.onChange()
  }

  private get emptyState(): JSX.Element {
    const {orgName} = this.props
    const {searchTerm} = this.state

    if (_.isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Medium}>
          <EmptyState.Text
            text={`${orgName} does not own any Telegraf  Configurations, why not create one?`}
            highlightWords={['Telegraf', 'Configurations']}
          />
          {this.createButton}
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text text="No Telegraf  Configuration buckets match your query" />
      </EmptyState>
    )
  }

  private handleDownloadConfig = async (telegrafID: string) => {
    try {
      const config = await getTelegrafConfigTOML(telegrafID)
      downloadTextFile(config, 'config.toml')
    } catch (error) {
      notify(getTelegrafConfigFailed())
    }
  }
  private handleDeleteTelegraf = async (telegrafID: string) => {
    await deleteTelegrafConfig(telegrafID)
    this.props.onChange()
  }

  private handleFilterChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private handleFilterBlur = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }
}
