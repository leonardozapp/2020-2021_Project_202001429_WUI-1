<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\TipoSituacaoFundo $tipoSituacaoFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('Edit Tipo Situacao Fundo'), ['action' => 'edit', $tipoSituacaoFundo->id], ['class' => 'side-nav-item']) ?>
            <?= $this->Form->postLink(__('Delete Tipo Situacao Fundo'), ['action' => 'delete', $tipoSituacaoFundo->id], ['confirm' => __('Are you sure you want to delete # {0}?', $tipoSituacaoFundo->id), 'class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('List Tipo Situacao Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('New Tipo Situacao Fundo'), ['action' => 'add'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="tipoSituacaoFundos view content">
            <h3><?= h($tipoSituacaoFundo->id) ?></h3>
            <table>
                <tr>
                    <th><?= __('Situacao') ?></th>
                    <td><?= h($tipoSituacaoFundo->situacao) ?></td>
                </tr>
                <tr>
                    <th><?= __('Id') ?></th>
                    <td><?= $this->Number->format($tipoSituacaoFundo->id) ?></td>
                </tr>
            </table>
            <div class="related">
                <h4><?= __('Related Situacao Fundos') ?></h4>
                <?php if (!empty($tipoSituacaoFundo->situacao_fundos)) : ?>
                <div class="table-responsive">
                    <table>
                        <tr>
                            <th><?= __('Cnpj Fundo Id') ?></th>
                            <th><?= __('Tipo Situacao Fundo Id') ?></th>
                            <th><?= __('DT INI SIT') ?></th>
                            <th><?= __('DT REG CVM') ?></th>
                            <th class="actions"><?= __('Actions') ?></th>
                        </tr>
                        <?php foreach ($tipoSituacaoFundo->situacao_fundos as $situacaoFundos) : ?>
                        <tr>
                            <td><?= h($situacaoFundos->cnpj_fundo_id) ?></td>
                            <td><?= h($situacaoFundos->tipo_situacao_fundo_id) ?></td>
                            <td><?= h($situacaoFundos->DT_INI_SIT) ?></td>
                            <td><?= h($situacaoFundos->DT_REG_CVM) ?></td>
                            <td class="actions">
                                <?= $this->Html->link(__('View'), ['controller' => 'SituacaoFundos', 'action' => 'view', $situacaoFundos->cnpj_fundo_id]) ?>
                                <?= $this->Html->link(__('Edit'), ['controller' => 'SituacaoFundos', 'action' => 'edit', $situacaoFundos->cnpj_fundo_id]) ?>
                                <?= $this->Form->postLink(__('Delete'), ['controller' => 'SituacaoFundos', 'action' => 'delete', $situacaoFundos->cnpj_fundo_id], ['confirm' => __('Are you sure you want to delete # {0}?', $situacaoFundos->cnpj_fundo_id)]) ?>
                            </td>
                        </tr>
                        <?php endforeach; ?>
                    </table>
                </div>
                <?php endif; ?>
            </div>
        </div>
    </div>
</div>
