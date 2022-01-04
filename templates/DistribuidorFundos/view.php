<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\DistribuidorFundo $distribuidorFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('Edit Distribuidor Fundo'), ['action' => 'edit', $distribuidorFundo->id], ['class' => 'side-nav-item']) ?>
            <?= $this->Form->postLink(__('Delete Distribuidor Fundo'), ['action' => 'delete', $distribuidorFundo->id], ['confirm' => __('Are you sure you want to delete # {0}?', $distribuidorFundo->id), 'class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('List Distribuidor Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('New Distribuidor Fundo'), ['action' => 'add'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="distribuidorFundos view content">
            <h3><?= h($distribuidorFundo->id) ?></h3>
            <table>
                <tr>
                    <th><?= __('Cnpj') ?></th>
                    <td><?= h($distribuidorFundo->cnpj) ?></td>
                </tr>
                <tr>
                    <th><?= __('Nome') ?></th>
                    <td><?= h($distribuidorFundo->nome) ?></td>
                </tr>
                <tr>
                    <th><?= __('Id') ?></th>
                    <td><?= $this->Number->format($distribuidorFundo->id) ?></td>
                </tr>
                <tr>
                    <th><?= __('DT REG CVM') ?></th>
                    <td><?= h($distribuidorFundo->DT_REG_CVM) ?></td>
                </tr>
            </table>
            <div class="related">
                <h4><?= __('Related Operacoes Financeiras') ?></h4>
                <?php if (!empty($distribuidorFundo->operacoes_financeiras)) : ?>
                <div class="table-responsive">
                    <table>
                        <tr>
                            <th><?= __('Id') ?></th>
                            <th><?= __('Usuario Id') ?></th>
                            <th><?= __('Cnpj Fundo Id') ?></th>
                            <th><?= __('Distribuidor Fundo Id') ?></th>
                            <th><?= __('Tipo Operacoes Financeira Id') ?></th>
                            <th><?= __('Por Valor') ?></th>
                            <th><?= __('Valor Total') ?></th>
                            <th><?= __('Valor Cota') ?></th>
                            <th><?= __('Quantidade Cotas') ?></th>
                            <th><?= __('Data') ?></th>
                            <th class="actions"><?= __('Actions') ?></th>
                        </tr>
                        <?php foreach ($distribuidorFundo->operacoes_financeiras as $operacoesFinanceiras) : ?>
                        <tr>
                            <td><?= h($operacoesFinanceiras->id) ?></td>
                            <td><?= h($operacoesFinanceiras->usuario_id) ?></td>
                            <td><?= h($operacoesFinanceiras->cnpj_fundo_id) ?></td>
                            <td><?= h($operacoesFinanceiras->distribuidor_fundo_id) ?></td>
                            <td><?= h($operacoesFinanceiras->tipo_operacoes_financeira_id) ?></td>
                            <td><?= h($operacoesFinanceiras->por_valor) ?></td>
                            <td><?= h($operacoesFinanceiras->valor_total) ?></td>
                            <td><?= h($operacoesFinanceiras->valor_cota) ?></td>
                            <td><?= h($operacoesFinanceiras->quantidade_cotas) ?></td>
                            <td><?= h($operacoesFinanceiras->data) ?></td>
                            <td class="actions">
                                <?= $this->Html->link(__('View'), ['controller' => 'OperacoesFinanceiras', 'action' => 'view', $operacoesFinanceiras->id]) ?>
                                <?= $this->Html->link(__('Edit'), ['controller' => 'OperacoesFinanceiras', 'action' => 'edit', $operacoesFinanceiras->id]) ?>
                                <?= $this->Form->postLink(__('Delete'), ['controller' => 'OperacoesFinanceiras', 'action' => 'delete', $operacoesFinanceiras->id], ['confirm' => __('Are you sure you want to delete # {0}?', $operacoesFinanceiras->id)]) ?>
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
